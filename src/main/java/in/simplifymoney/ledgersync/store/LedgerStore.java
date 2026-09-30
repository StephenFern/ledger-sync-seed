package in.simplifymoney.ledgersync.store;

import in.simplifymoney.ledgersync.model.NormalizedTxn;
import java.util.List;

/**
 * Where transactions live.
 *
 * Note what this interface does NOT promise: that saving the same transaction
 * twice results in one row.
 */
public interface LedgerStore {

//    void save(NormalizedTxn txn);
    
    void saveBatch(List<NormalizedTxn> transactions);

    List<NormalizedTxn> all();

    long count();
}
