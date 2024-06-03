package edu.stanford.nami.examples;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Timer;
import com.codahale.metrics.UniformReservoir;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import edu.stanford.nami.Account;
import edu.stanford.nami.NKey;
import edu.stanford.nami.NamiClient;
import edu.stanford.nami.TransactionResponse;
import edu.stanford.nami.TransactionStatus;
import edu.stanford.nami.client.ClientMetrics;
import edu.stanford.nami.client.ClientTransaction;
import edu.stanford.nami.client.NamiClientTransaction;
import edu.stanford.nami.config.ChunksConfig;
import edu.stanford.nami.config.ClientConfig;
import edu.stanford.nami.config.Config;
import edu.stanford.nami.config.PeersConfig;
import edu.stanford.nami.nativerocksdb.NativeRocksDBClient;
import edu.stanford.nami.nativerocksdb.NativeRocksDBClientTransaction;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import lombok.RequiredArgsConstructor;
import lombok.experimental.UtilityClass;
import lombok.extern.flogger.Flogger;

@Flogger
@RequiredArgsConstructor
public final class BankingApp {
  @UtilityClass
  private static final class Metrics {
    Histogram accountSize =
        ClientMetrics.registry.histogram(
            "banking-app.accountSize", () -> new Histogram(new UniformReservoir()));
    Timer moveMoney =
        ClientMetrics.registry.timer(
            "banking-app.moveMoney", () -> new Timer(new UniformReservoir()));
  }

  public static final int THREADS = 30;
  public static final int ACCOUNTS = 30000;
  public static final int TX_PER_THREAD = 400;
  public static final int MOVES_PER_TX = 5;
  public static final int MAX_MOVED_AMOUNT = 100;
  public static final int MAX_RETRIES = 20;
  public static final int GARBAGE_LENGTH = 1000;
  public static final int ACCOUNT_CREATION_BATCH_SIZE = 500;

  private static final Random random = new Random();

  public final Optional<NamiClient> namiClient;
  public final Optional<NativeRocksDBClient> nativeRocksClient;

  private final AtomicLong latestTid = new AtomicLong(0L);

  public static void main(String[] args) throws InterruptedException {
    log.atInfo().log("Starting BankingApp benchmark");
    log.atInfo().log("Running in " + new File(".").getAbsolutePath());

    if (args.length != 2) {
      log.atSevere().log("Invalid usage. Usage: banking-app <Nami|Rocks> <config_file>");
      System.exit(-1);
    }
    boolean isNami = Objects.equals(args[0], "Nami");
    var configFileName = args[1];
    var configFile = new File(configFileName);

    if (!configFile.exists()) {
      log.atSevere().log("File " + configFile.getAbsolutePath() + " does not exist");
      System.exit(-2);
    } else {
      log.atInfo().log("Found config file at " + configFile.getAbsolutePath());
    }

    var config = loadClientConfig(configFile);
    log.atInfo().log("Loaded client config " + config);
    var peersConfig = loadPeersConfig(configFile, config.getPeerConfigsPath());
    var chunksConfig = loadChunksConfig(configFile, config.getChunkConfigPath());
    log.atInfo().log("Setting up metrics");
    var metricsDirectory = Config.resolveRelativeToConfigFile(configFile, config.getMetricsPath());
    ClientMetrics.startReporting(metricsDirectory);

    if (isNami) {
      try (NamiClient namiClient = new NamiClient(peersConfig, chunksConfig)) {
        new BankingApp(Optional.of(namiClient), Optional.empty()).run();
        log.atInfo().log("Flushing metrics");
        ClientMetrics.awaitOneLastReport();
        log.atInfo().log("Done running banking app");
      } catch (Exception e) {
        e.printStackTrace();
      }
    } else {
      var target = peersConfig.getPeers().getFirst().getKvAddress();
      var channel = Grpc.newChannelBuilder(target, InsecureChannelCredentials.create()).build();
      NativeRocksDBClient rocksDBClient = new NativeRocksDBClient(channel);
      new BankingApp(Optional.empty(), Optional.of(rocksDBClient)).run();
      log.atInfo().log("Flushing metrics");
      ClientMetrics.awaitOneLastReport();
      log.atInfo().log("Done running banking app");
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
    recreateTimers("setup");
    var accountKeys = createAccounts();

    // INVARIANT: all account balances should add up to zero
    validateZeroNetBalance(accountKeys);

    // Reset metrics so we measure only money movements
    recreateTimers("benchmark");

    var workers = new ArrayList<Worker>();
    for (int i = 0; i < THREADS; i++) {
      var worker = new Worker(i, accountKeys);
      workers.add(worker);
      log.atInfo().log("Creating worker " + i);
      worker.start();
    }
    for (var worker : workers) {
      log.atInfo().log("Waiting on worker " + worker.workerIndex);
      worker.join();
    }

    recreateTimers("teardown");

    // INVARIANT: after all movements, all balances should add up to zero
    validateZeroNetBalance(accountKeys);
  }

  public ClientTransaction begin(Optional<Long> snapshotTid) {
    if (namiClient.isPresent()) {
      return NamiClientTransaction.begin(namiClient.get(), snapshotTid);
    } else if (nativeRocksClient.isPresent()) {
      return NativeRocksDBClientTransaction.begin(nativeRocksClient.get());
    }
    throw new RuntimeException("Could not find either nami or rocks db client");
  }

  public void recreateTimers(String prefix) {
    if (namiClient.isPresent()) {
      NamiClient.Timers.recreateTimers(prefix);
    } else if (nativeRocksClient.isPresent()) {
      NativeRocksDBClient.Timers.recreateTimers(prefix);
    } else {
      throw new RuntimeException("Could not find either nami or rocks db client");
    }
  }

  /** Create ACCOUNT accounts with UUIDs as keys, and a balance of zero. */
  private List<String> createAccounts() {
    var accountKeys = new ArrayList<String>();
    var tx = begin(Optional.empty());
    int accountsInBatch = 0;
    for (int i = 0; i < ACCOUNTS; i++) {
      var accountKey = UUID.randomUUID().toString();
      // zero out all balances
      writeBalance(tx, accountKey, 0);
      accountKeys.add(accountKey);
      accountsInBatch++;
      if (accountsInBatch >= ACCOUNT_CREATION_BATCH_SIZE) {
        log.atInfo().log("Creating %s accounts", accountsInBatch);
        var response = tx.commit();
        updateLatestTid(response.getTid());
        tx = begin(Optional.empty());
        accountsInBatch = 0;
      }
    }

    if (accountsInBatch > 0) {
      log.atInfo().log("Creating %s accounts", accountsInBatch);
      var response = tx.commit();
      updateLatestTid(response.getTid());
    }

    return accountKeys;
  }

  /** Validate that, in total, all accounts still have zero balance. */
  private void validateZeroNetBalance(List<String> accountKeys) {
    // begin tx so we know all values are consistent
    var tx = begin(Optional.of(this.latestTid.get()));
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
    log.atInfo().log("Positive balance: " + positiveBalance);
    log.atInfo().log("Negative balance: " + negativeBalance);
    Preconditions.checkState(
        positiveBalance + negativeBalance == 0, "Net balance for accounts was not zero");
  }

  private void updateLatestTid(long newTid) {
    long oldTid = latestTid.get();
    while (newTid > oldTid) {
      if (latestTid.compareAndSet(oldTid, newTid)) {
        break;
      }
      oldTid = latestTid.get();
    }
  }

  @RequiredArgsConstructor
  public class Worker extends Thread {
    private final int workerIndex;
    private final List<String> accountKeys;
    private final Random random = new Random();

    @Override
    public void run() {
      log.atInfo().log("Starting worker " + workerIndex);
      for (int i = 0; i < TX_PER_THREAD; i++) {
        log.atInfo().log("Worker " + workerIndex + " moving money");
        try (var timer = Metrics.moveMoney.time()) {
          moveMoney();
        }
      }
      log.atInfo().log("Worker " + workerIndex + " completed");
    }

    private void moveMoney() {
      int numRetries = 0;
      while (numRetries < MAX_RETRIES) {
        var tx = begin(Optional.empty());
        moveMoneyInTransaction(tx);
        TransactionResponse outcome = tx.commit();
        TransactionStatus status = outcome.getStatus();
        if (status == TransactionStatus.UNKNOWN) {
          throw new RuntimeException("GOT UNKNOWN TRANSACTION!");
        }
        updateLatestTid(outcome.getTid());
        if (status == TransactionStatus.COMMITTED) {
          break;
        }
        log.atInfo().log(
            "Worker %s encountered a conflict, attempt %s, retrying...", workerIndex, numRetries);
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
    var bytes = tx.get(new NKey(accountKey));
    Account account;
    try {
      account = Account.parseFrom(bytes);
    } catch (InvalidProtocolBufferException e) {
      log.atSevere().log("Error parsing account contents for accountKey %s", accountKey, e);
      throw new RuntimeException(e);
    }
    return account.getBalance();
  }

  private void writeBalance(ClientTransaction tx, String accountKey, long balance) {
    var builder = Account.newBuilder();
    builder.setAccountId(accountKey);
    builder.setBalance(balance);
    builder.setDescription(randomDescription(GARBAGE_LENGTH));
    ByteString payload = builder.build().toByteString();
    Metrics.accountSize.update(payload.size());
    tx.put(new NKey(accountKey), payload);
  }

  private static String randomDescription(int length) {
    var builder = new StringBuilder();
    for (int i = 0; i < length; i++) {
      var c = random.nextInt('a', 'z');
      builder.append((char) c);
    }
    return builder.toString();
  }
}
