package js.transaction.eclipselink;

import java.io.IOException;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import js.lang.Config;
import js.lang.ConfigException;
import js.transaction.Transaction;
import js.transaction.TransactionException;
import js.transaction.TransactionManager;
import js.transaction.WorkingUnit;
import js.util.Classes;
import js.util.Strings;

public class TransactionManagerImpl implements TransactionManager
{
  private static final String PUNAME_MARK = "name=\"";

  /** Keep current on working transaction on thread local variable. */
  private final ThreadLocal<TransactionImpl> transactionsCache = new ThreadLocal<>();

  private EntityManagerFactory factory;

  public TransactionManagerImpl()
  {
  }

  public TransactionManagerImpl(String persistenceUnitName)
  {
    factory = Persistence.createEntityManagerFactory(persistenceUnitName);
  }

  @Override
  public void config(Config config) throws ConfigException
  {
    String persistence = null;
    try {
      persistence = Strings.load(Classes.getResourceAsReader("/META-INF/persistence.xml"));
    }
    catch(IOException e) {
      throw new ConfigException(e);
    }

    int start = persistence.indexOf(PUNAME_MARK);
    if(start == -1) {
      throw new ConfigException("Invalid persistence configuration. Missing persistence unit name.");
    }
    start += PUNAME_MARK.length();

    int end = persistence.indexOf('"', start);
    if(end == -1) {
      throw new ConfigException("Invalid persistence configuration. Missing persistence unit name.");
    }

    factory = Persistence.createEntityManagerFactory(persistence.substring(start, end));
  }

  @Override
  public Transaction createTransaction()
  {
    return createTransaction(false);
  }

  @Override
  public Transaction createReadOnlyTransaction()
  {
    return createTransaction(true);
  }

  private TransactionImpl createTransaction(boolean readOnly)
  {
    TransactionImpl transaction = transactionsCache.get();
    if(transaction != null) {
      transaction.incrementTransactionNestingLevel();
    }
    else {
      transaction = new TransactionImpl(this, readOnly);
      transactionsCache.set(transaction);
    }
    return transaction;
  }

  /** Remote transaction from transactions cache. */
  public void destroyTransaction()
  {
    transactionsCache.set(null);
  }

  public EntityManager getEntityManager()
  {
    return factory.createEntityManager();
  }

  @SuppressWarnings("unchecked")
  @Override
  public <S, T> T exec(WorkingUnit<S, T> workingUnit, Object... args)
  {
    Transaction t = createTransaction();
    T o = null;
    try {
      o = (T)workingUnit.exec((S)t.getSession(), args);
      t.commit();
    }
    catch(Exception e) {
      t.rollback();
      throw new TransactionException(e);
    }
    finally {
      t.close();
    }
    return o;
  }

  @Override
  public void destroy()
  {
    factory.close();
  }
}
