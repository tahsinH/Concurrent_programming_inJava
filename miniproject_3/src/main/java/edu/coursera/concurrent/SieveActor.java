package edu.coursera.concurrent;

import edu.rice.pcdp.Actor;

import static edu.rice.pcdp.PCDP.finish;


/**
 * An actor-based implementation of the Sieve of Eratosthenes.
 * <p>
 * TODO Fill in the empty SieveActorActor actor class below and use it from
 * countPrimes to determine the number of primes <= limit.
 */
public final class SieveActor extends Sieve {


    /**
     * {@inheritDoc}
     * <p>
     * TODO Use the SieveActorActor class to calculate the number of primes <=
     * limit in parallel. You might consider how you can model the Sieve of
     * Eratosthenes as a pipeline of actors, each corresponding to a single
     * prime number.
     */
    @Override
    public int countPrimes(final int limit) {
        final SieveActorActor sieveActor = new SieveActorActor(0, 2);

        // pump the first actor in the chain
        finish(() -> {

            // Note: 1 is not a prime number;
            // 2, is the only even prime number.
            // just send odd numbers, since multiple of 2 is already being filtered.
            for (int i = 3; i <= limit; i += 2) {
                sieveActor.send(i);
            }
            sieveActor.send(0);
        });


        // add up the results from the actor chain
        int totalPrimes = 0;
        // the rest of the primes will come from
        SieveActorActor currActor = sieveActor;

        do {
            totalPrimes += currActor.numLocalPrimes();
            currActor = currActor.nextActor();
        } while (currActor != null);

        return totalPrimes;
    }


    /**
     * An actor class that helps implement the Sieve of Eratosthenes in
     * parallel.
     */
    public static final class SieveActorActor extends Actor {
        private int actorLevelId;

        private static int MAX_LOCAL_PRIMES = 500;
        private final int localPrimes[];  // we actually don't need this. More for debug purpose

        private int numLocalPrimes;

        private int localPrime;
        private SieveActorActor nextActor;


        public SieveActorActor(final int actorLevel, final int primeToSearch) {
            localPrime = primeToSearch;
            actorLevelId = actorLevel;

            this.localPrimes = new int[MAX_LOCAL_PRIMES];
            localPrimes[0] = localPrime;
            this.numLocalPrimes = 1;
        }

        public int numLocalPrimes() {
            return numLocalPrimes;
        }

        public SieveActorActor nextActor() {
            return nextActor;
        }

        /**
         * Process a single message sent to this actor.
         * <p>
         * TODO complete this method.
         *
         * @param msg Received message
         */
        @Override
        public void process(final Object msg) {
            final int candidate = (Integer) msg;

            // check for exit condition
            if (candidate <= 0) {
                if (nextActor != null) {
                    nextActor.send(0);
                }
                return;
            } else {
                final boolean locallyPrime = isLocallyPrime(candidate);
                if (locallyPrime) {
                    if (numLocalPrimes < MAX_LOCAL_PRIMES) {
                        localPrimes[numLocalPrimes] = candidate;
                        numLocalPrimes += 1;
                    } else if (nextActor == null) {
                        nextActor = new SieveActorActor(actorLevelId + 1, candidate);
                    } else {
                        nextActor.send(msg);
                    }
                }
            }
        }// SieveActorActor - process

        private boolean isLocallyPrime(int candidate) {
            boolean isPrime = true;
            for (int i = 0; i < numLocalPrimes; i++) {
                if (candidate % localPrimes[i] == 0) {
                    isPrime = false;
                    break;
                }
            }
            return isPrime;
        }

    } // SieveActorActor
} // SieveActor

