package in.simplifymoney.ledgersync.ingest;

import in.simplifymoney.ledgersync.json.Json;
import in.simplifymoney.ledgersync.model.Category;
import in.simplifymoney.ledgersync.model.Direction;
import in.simplifymoney.ledgersync.model.NormalizedTxn;
import in.simplifymoney.ledgersync.model.RawMessage;
import in.simplifymoney.ledgersync.parse.ParsedTxn;
import in.simplifymoney.ledgersync.parse.Parsers;
import in.simplifymoney.ledgersync.store.LedgerStore;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import java.math.BigDecimal;
/**
 * Reads a corpus of raw messages and puts transactions in the ledger.
 *
 * This is the naive version. It parses each message on its own and saves
 * whatever comes back. It does not ask whether two messages describe the same
 * transaction, and it decides the category from the direction alone.
 */
public final class IngestService {

    private final Parsers parsers;
    private final LedgerStore store;

    public IngestService(Parsers parsers, LedgerStore store) {
        this.parsers = parsers;
        this.store = store;
    }

    public Stats ingestFile(Path corpus) throws IOException {
        List<RawMessage> messages = readCorpus(corpus);
        int parsed = 0;
        int skipped = 0;
        int count = 0; 
        List<NormalizedTxn> batch = new ArrayList<>(500);
        for (RawMessage m : messages) {
            Optional<ParsedTxn> p = parsers.parse(m);
            if (p.isEmpty()) {
                skipped++;
                continue;
            }
            if (p.get().amount().compareTo(p.get().statedBalance()) == 0)
            {
                count++;
                System.out.println("AFFECTED:");
                System.out.println("messageId = " + m.messageId());
                System.out.println("body = " + m.body());
                System.out.println("amount = " + p.get().amount());
                System.out.println("balance = " + p.get().statedBalance());
                System.out.println();
            }
            batch.add(toTransaction(p.get()));
            parsed++;

            if (batch.size() == 500) {
                store.saveBatch(batch);
                batch.clear();
            }
        }
        
        if (!batch.isEmpty()) {
            store.saveBatch(batch);
        }
        batch.clear();
        System.out.println("count : "+count);
        
        return new Stats(messages.size(), parsed, skipped);
    }

    public static List<RawMessage> readCorpus(Path corpus) throws IOException {
        List<RawMessage> out = new ArrayList<>();
        try (Stream<String> lines = Files.lines(corpus)) {
            for (String line : (Iterable<String>) lines.filter(s -> !s.isBlank())::iterator) {
                Map<String, Object> o = Json.parseObject(line);
                out.add(new RawMessage(
                        (String) o.get("message_id"),
                        (String) o.get("channel"),
                        (String) o.get("sender"),
                        OffsetDateTime.parse((String) o.get("received_at")),
                        (String) o.get("device_id"),
                        (String) o.get("body")));
            }
        }
        return out;
    }

    private NormalizedTxn toTransaction(ParsedTxn p) {
        Category c = p.direction() == Direction.DEBIT ? Category.SPEND : Category.INCOME;
        return new NormalizedTxn(p.accountLast4(), p.occurredAt(), p.direction(),
                p.amount(), c, p.merchant(), List.of(p.sourceMessageId()));
    }

    public record Stats(int messagesRead, int transactionsWritten, int messagesSkipped) {}
}
