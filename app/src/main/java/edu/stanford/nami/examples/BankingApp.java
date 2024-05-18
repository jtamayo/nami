package edu.stanford.nami.examples;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

import edu.stanford.nami.NKey;
import edu.stanford.nami.NamiClient;
import edu.stanford.nami.client.ClientTransaction;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import lombok.RequiredArgsConstructor;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@RequiredArgsConstructor
public final class BankingApp {
  public static final int THREADS = 1;
  public static final int ACCOUNTS = 1000;
  public static final int TX_PER_THREAD = 100;
  public static final int MOVES_PER_TX = 10;
  public static final int MAX_MOVED_AMOUNT = 100;

  private final NamiClient client;

  public static void main(String[] args) throws InterruptedException {
    System.out.println("Starting BankingApp benchmark");
    String targetHost = "localhost";
    int defaultPort = 8980;
    if (args.length > 0) {
      final int serverPeerIndex = Integer.parseInt(args[0]);
      if (serverPeerIndex < 0 || serverPeerIndex > 2) {
        throw new IllegalArgumentException(
            "The server index must be 0, 1 or 2: peerIndex=" + serverPeerIndex);
      }
      defaultPort += serverPeerIndex;
    }
    String target = targetHost + ":" + defaultPort;

    ManagedChannel channel =
        Grpc.newChannelBuilder(target, InsecureChannelCredentials.create()).build();
    try (NamiClient client = new NamiClient(channel)) {
      new BankingApp(client).run();
      System.out.println("Done running banking app");
    } catch (Throwable e) {
      e.printStackTrace();
    } finally {
      channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  public void run() throws InterruptedException {
    var accountKeys = createAccounts();
    // INVARIANT: all account balances should add up to zero
    validateZeroNetBalance(accountKeys);

    var workers = new ArrayList<Worker>();
    for (int i = 0; i < THREADS; i++) {
      var worker = new Worker(i, accountKeys);
      workers.add(worker);
      System.out.println("Creating worker " + i);
      worker.start();
    }
    for (var worker : workers) {
      System.out.println("Waiting on worker " + worker.workerIndex);
      worker.join();
    }
    // INVARIANT: after all movements, all balances should add up to zero
    validateZeroNetBalance(accountKeys);
  }

  /** Create ACCOUNT accounts with UUIDs as keys, and a balance of zero. */
  private List<String> createAccounts() {
    var accountKeys = new ArrayList<String>();
    var tx = ClientTransaction.begin(client);
    for (int i = 0; i < ACCOUNTS; i++) {
      var accountKey = UUID.randomUUID().toString();
      // zero out all balances
      System.out.println("Creating account " + accountKey);
      writeBalance(tx, accountKey, 0);
      accountKeys.add(accountKey);
    }

    return accountKeys;
  }

  /** Validate that, in total, all accounts still have zero balance. */
  private void validateZeroNetBalance(List<String> accountKeys) {
    // begin tx so we know all values are consistent
    var tx = ClientTransaction.begin(client);
    var netBalance = 0L;
    for (String accountKey : accountKeys) {
      netBalance += readBalance(tx, accountKey);
    }
    Preconditions.checkState(netBalance == 0, "Net balance for accounts was not zero");
  }

  @RequiredArgsConstructor
  public class Worker extends Thread {
    private final int workerIndex;
    private final List<String> accountKeys;
    private final Random random = new Random();

    @Override
    public void run() {
      System.out.println("Starting worker " + workerIndex);
      for (int i = 0; i < TX_PER_THREAD; i++) {
        System.out.println("Worker " + workerIndex + " moving money");
        moveMoney();
      }
      System.out.println("Worker " + workerIndex + " completed");
    }

    private void moveMoney() {
      var maxIndex = accountKeys.size();
      var tx = ClientTransaction.begin(client);

      for (int i = 0; i < MOVES_PER_TX; i++) {
        // pick two accounts at random, move random amount between them
        var fromIndex = random.nextInt(maxIndex);
        var fromAccount = accountKeys.get(fromIndex);
        // add maxIndex-1 and wrap around so we never get the same account
        var toIndex = (fromIndex + random.nextInt(maxIndex - 1) + 1) % maxIndex;
        var toAccount = accountKeys.get(toIndex);
        // paranoia: check accounts are different
        Preconditions.checkState(toIndex != fromIndex);
        Preconditions.checkState(!toAccount.equals(fromAccount));

        var transferAmount = random.nextInt(MAX_MOVED_AMOUNT);

        var oldFromBalance = readBalance(tx, fromAccount);
        var oldToBalance = readBalance(tx, toAccount);

        writeBalance(tx, fromAccount, oldFromBalance - transferAmount);
        writeBalance(tx, toAccount, oldToBalance + transferAmount);
      }

      tx.commit();
    }
  }

  private long readBalance(ClientTransaction tx, String accountKey) {
    ByteBuffer value = tx.get(new NKey(accountKey)).asReadOnlyByteBuffer();
    var balance = value.getLong();
    // paranoia: check we read it all
    Preconditions.checkState(!value.hasRemaining());
    return balance;
  }

  private void writeBalance(ClientTransaction tx, String accountKey, long balance) {
    var byteBuffer = ByteBuffer.allocate(8);
    byteBuffer.putLong(balance);
    byteBuffer.rewind();
    tx.put(new NKey(accountKey), ByteString.copyFrom(byteBuffer));
  }
}
