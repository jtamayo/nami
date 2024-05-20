package edu.stanford.nami.examples;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import edu.stanford.nami.NKey;
import edu.stanford.nami.NamiClient;
import edu.stanford.nami.TransactionStatus;
import edu.stanford.nami.client.ClientTransaction;
import edu.stanford.nami.config.ChunksConfig;
import edu.stanford.nami.config.ClientConfig;
import edu.stanford.nami.config.Config;
import edu.stanford.nami.config.PeersConfig;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public final class BankingApp {
  public static final int THREADS = 10;
  public static final int ACCOUNTS = 100;
  public static final int TX_PER_THREAD = 10;
  public static final int MOVES_PER_TX = 10;
  public static final int MAX_MOVED_AMOUNT = 100;
  public static final int MAX_RETRIES = 20;

  private final NamiClient client;

  public static void main(String[] args) throws InterruptedException {
    System.out.println("Starting BankingApp benchmark");
    System.out.println("Running in " + new File(".").getAbsolutePath());

    if (args.length != 1) {
      System.err.println("Invalid usage. Usage: banking-app <config_file>");
      System.exit(-1);
    }
    var configFileName = args[0];
    var configFile = new File(configFileName);

    if (!configFile.exists()) {
      System.err.println("File " + configFile.getAbsolutePath() + " does not exist");
      System.exit(-2);
    } else {
      System.out.println("Found config file at " + configFile.getAbsolutePath());
    }

    var config = loadClientConfig(configFile);
    System.out.println("Loaded client config " + config);
    var peersConfig = loadPeersConfig(configFile, config.getPeerConfigsPath());
    var chunksConfig = loadChunksConfig(configFile, config.getChunkConfigPath());

    // TODO connect to other peers besides the first one
    var firstPeerConfig = peersConfig.getPeers().get(0);

    String target = firstPeerConfig.getKvAddress();

    ManagedChannel channel =
        Grpc.newChannelBuilder(target, InsecureChannelCredentials.create()).build();
    try (NamiClient client = new NamiClient(peersConfig, chunksConfig)) {
      new BankingApp(client).run();
      System.out.println("Done running banking app");
    } catch (Throwable e) {
      e.printStackTrace();
    } finally {
      channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  private static ClientConfig loadClientConfig(File configFile) {
    try (var reader = Files.newReader(configFile, Charsets.UTF_8)) {
      return new Gson().fromJson(reader, ClientConfig.class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static PeersConfig loadPeersConfig(File configFile, String path) {
    return Config.loadConfig(configFile, path, PeersConfig.class);
  }

  public static ChunksConfig loadChunksConfig(File configFile, String path) {
    return Config.loadConfig(configFile, path, ChunksConfig.class);
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
    tx.commit();

    return accountKeys;
  }

  /** Validate that, in total, all accounts still have zero balance. */
  private void validateZeroNetBalance(List<String> accountKeys) {
    // begin tx so we know all values are consistent
    var tx = ClientTransaction.begin(client);
    var positiveBalance = 0L;
    var negativeBalance = 0L;
    for (String accountKey : accountKeys) {
      var balance = readBalance(tx, accountKey);
      if (balance > 0) {
        positiveBalance += balance;
      } else {
        negativeBalance += balance;
      }
    }
    System.out.println("Positive balance: " + positiveBalance);
    System.out.println("Negative balance: " + negativeBalance);
    Preconditions.checkState(
        positiveBalance + negativeBalance == 0, "Net balance for accounts was not zero");
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
      int numRetries = 0;
      while (numRetries < MAX_RETRIES) {
        var tx = ClientTransaction.begin(client);
        moveMoneyInTransaction(tx);
        TransactionStatus outcome = tx.commit();
        if (outcome == TransactionStatus.COMMITTED) {
          break;
        }
        System.out.println("Worker " + workerIndex + " encountered a conflict, retrying...");
        numRetries++;
      }
    }

    private void moveMoneyInTransaction(ClientTransaction tx) {
      var maxIndex = accountKeys.size();

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
