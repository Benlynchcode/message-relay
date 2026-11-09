package io.github.benlynchcode.relay.mailbox;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

// No sockets and no sleeps: the mailbox is pure logic, and the doorbell is just a counter here.
@Timeout(10)
class MailboxTest {

    private final AtomicInteger rings = new AtomicInteger();
    private final Runnable doorbell = rings::incrementAndGet;

    private final Mailbox bob = new Mailbox("bob", 3, 10);

    // ------------------------------------------------------------------ happy path and order

    @Test
    void enqueueDeliverAckEmptiesTheMailbox() {
        long ticket = bob.attach(doorbell).ticket();
        assertEquals(EnqueueResult.ACCEPTED, bob.enqueue("alice", "1", "hi"));

        assertEquals(new Envelope("alice", "1", "hi"), bob.nextForDelivery(ticket).orElseThrow());
        assertTrue(bob.ack("alice", "1"));

        assertEquals(0, bob.pendingCount());
        assertEquals(0, bob.inFlightCount());
    }

    @Test
    void deliversInFifoOrderOneAtATime() {
        long ticket = bob.attach(doorbell).ticket();
        bob.enqueue("alice", "1", "a");
        bob.enqueue("alice", "2", "b");

        assertEquals("1", bob.nextForDelivery(ticket).orElseThrow().id());
        // Max in-flight is 1: nothing more until the first is acknowledged.
        assertEquals(Optional.empty(), bob.nextForDelivery(ticket));

        bob.ack("alice", "1");
        assertEquals("2", bob.nextForDelivery(ticket).orElseThrow().id());
    }

    @Test
    void offlineMessagesAreQueuedAndReportedOnAttach() {
        bob.enqueue("alice", "1", "a");
        bob.enqueue("carol", "1", "b");

        assertEquals(2, bob.attach(doorbell).pending());
    }

    // ------------------------------------------------------------------ Q2: reconnects and tickets

    @Test
    void detachRequeuesInFlightAtTheFront() {
        long ticket = bob.attach(doorbell).ticket();
        bob.enqueue("alice", "1", "first");
        bob.enqueue("alice", "2", "second");
        bob.nextForDelivery(ticket);              // "1" is now in flight

        bob.detach(ticket);                       // disconnect without ACK
        long next = bob.attach(doorbell).ticket();

        assertEquals("1", bob.nextForDelivery(next).orElseThrow().id());   // redelivered first
    }

    @Test
    void takeoverRequeuesWithoutWaitingForTheOldConnection() {
        long oldTicket = bob.attach(doorbell).ticket();
        bob.enqueue("alice", "1", "a");
        bob.nextForDelivery(oldTicket);           // in flight on the old connection

        long newTicket = bob.attach(doorbell).ticket();   // new connection wins; old never detached

        assertEquals("1", bob.nextForDelivery(newTicket).orElseThrow().id());
    }

    @Test
    void staleTicketCanNeitherDeliverNorDetach() {
        long oldTicket = bob.attach(doorbell).ticket();
        long newTicket = bob.attach(doorbell).ticket();
        bob.enqueue("alice", "1", "a");

        // The old writer wakes up late: it must not pull the message onto a dead socket.
        assertEquals(Optional.empty(), bob.nextForDelivery(oldTicket));

        bob.nextForDelivery(newTicket);           // in flight on the NEW connection
        bob.detach(oldTicket);                    // old connection's late cleanup

        assertEquals(1, bob.inFlightCount());     // untouched
        assertTrue(bob.ack("alice", "1"));
    }

    @Test
    void nothingIsDeliveredAfterDetach() {
        long ticket = bob.attach(doorbell).ticket();
        bob.detach(ticket);
        bob.enqueue("alice", "1", "a");

        assertEquals(Optional.empty(), bob.nextForDelivery(ticket));
    }

    // ------------------------------------------------------------------ Q3/Q4: limits and duplicates

    @Test
    void fullMailboxRejectsAndRejectedIdCanBeRetried() {
        long ticket = bob.attach(doorbell).ticket();
        bob.enqueue("alice", "1", "a");
        bob.enqueue("alice", "2", "b");
        bob.enqueue("alice", "3", "c");

        assertEquals(EnqueueResult.MAILBOX_FULL, bob.enqueue("alice", "4", "d"));

        bob.nextForDelivery(ticket);
        bob.ack("alice", "1");                    // room again

        // Not DUPLICATE: a rejected id was never recorded.
        assertEquals(EnqueueResult.ACCEPTED, bob.enqueue("alice", "4", "d"));
    }

    @Test
    void inFlightCountsTowardsTheLimit() {
        long ticket = bob.attach(doorbell).ticket();
        bob.enqueue("alice", "1", "a");
        bob.nextForDelivery(ticket);              // 1 in flight, never ACKed
        bob.enqueue("alice", "2", "b");
        bob.enqueue("alice", "3", "c");

        assertEquals(EnqueueResult.MAILBOX_FULL, bob.enqueue("alice", "4", "d"));
    }

    @Test
    void retryWithSameBodyIsDuplicateAndNotStoredTwice() {
        bob.enqueue("alice", "1", "hi");

        assertEquals(EnqueueResult.DUPLICATE, bob.enqueue("alice", "1", "hi"));
        assertEquals(1, bob.pendingCount());
    }

    @Test
    void sameIdWithDifferentBodyIsIdReused() {
        bob.enqueue("alice", "1", "hi");

        assertEquals(EnqueueResult.ID_REUSED, bob.enqueue("alice", "1", "something else"));
        assertEquals(1, bob.pendingCount());
    }

    @Test
    void duplicateIsReportedEvenWhenTheMailboxIsFull() {
        bob.enqueue("alice", "1", "a");
        bob.enqueue("alice", "2", "b");
        bob.enqueue("alice", "3", "c");

        // The retry of a message we already hold must not be told MAILBOX_FULL.
        assertEquals(EnqueueResult.DUPLICATE, bob.enqueue("alice", "1", "a"));
    }

    @Test
    void retryAfterAckIsStillCaughtWithinTheWindow() {
        long ticket = bob.attach(doorbell).ticket();
        bob.enqueue("alice", "1", "a");
        bob.nextForDelivery(ticket);
        bob.ack("alice", "1");

        assertEquals(EnqueueResult.DUPLICATE, bob.enqueue("alice", "1", "a"));
        assertEquals(0, bob.pendingCount());
    }

    @Test
    void sameIdFromDifferentSendersAreDifferentMessages() {
        assertEquals(EnqueueResult.ACCEPTED, bob.enqueue("alice", "1", "a"));
        assertEquals(EnqueueResult.ACCEPTED, bob.enqueue("carol", "1", "b"));
    }

    @Test
    void dedupeWindowSmallerThanMailboxIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> new Mailbox("bob", 10, 9));
    }

    // ------------------------------------------------------------------ acks

    @Test
    void staleRepeatedAndWrongAcksChangeNothing() {
        long ticket = bob.attach(doorbell).ticket();
        bob.enqueue("alice", "1", "a");
        bob.enqueue("alice", "2", "b");
        bob.nextForDelivery(ticket);              // "1" in flight, "2" pending

        assertFalse(bob.ack("alice", "2"));       // pending, never delivered
        assertFalse(bob.ack("carol", "1"));       // right id, wrong sender
        assertFalse(bob.ack("alice", "99"));      // never existed
        assertTrue(bob.ack("alice", "1"));
        assertFalse(bob.ack("alice", "1"));       // repeated

        assertEquals(1, bob.pendingCount());
        assertEquals(0, bob.inFlightCount());
    }

    // ------------------------------------------------------------------ Q9: doorbell

    @Test
    void doorbellRingsOnAttachEnqueueAndAckOnly() {
        bob.enqueue("alice", "1", "a");
        assertEquals(0, rings.get());             // offline: nobody to ring

        long ticket = bob.attach(doorbell).ticket();
        assertEquals(1, rings.get());             // attach: drain what queued while offline

        bob.enqueue("alice", "2", "b");
        assertEquals(2, rings.get());             // new mail

        bob.nextForDelivery(ticket);
        assertEquals(2, rings.get());             // taking a message doesn't ring

        bob.ack("alice", "1");
        assertEquals(3, rings.get());             // slot freed

        bob.ack("alice", "1");
        assertEquals(3, rings.get());             // stale ack: nothing changed, no ring
    }

    // ------------------------------------------------------------------ concurrency

    @Test
    void concurrentRetriesOfOneMessageStoreItExactlyOnce() throws Exception {
        Mailbox mailbox = new Mailbox("bob", 100, 100);
        int threads = 16;
        CountDownLatch start = new CountDownLatch(1);
        List<Future<EnqueueResult>> results = new ArrayList<>();

        try (ExecutorService pool = Executors.newFixedThreadPool(threads)) {
            for (int i = 0; i < threads; i++) {
                results.add(pool.submit(() -> {
                    start.await();                // all threads released together
                    return mailbox.enqueue("alice", "1", "hi");
                }));
            }
            start.countDown();

            int accepted = 0;
            for (Future<EnqueueResult> result : results) {
                if (result.get() == EnqueueResult.ACCEPTED) {
                    accepted++;
                }
            }
            // Check-then-store is one locked step, so exactly one thread wins.
            assertEquals(1, accepted);
            assertEquals(1, mailbox.pendingCount());
        }
    }
}
