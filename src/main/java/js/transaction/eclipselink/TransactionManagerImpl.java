package js.transaction.eclipselink;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import js.lang.BugError;
import js.lang.Config;
import js.lang.ConfigException;
import js.log.Log;
import js.log.LogFactory;
import js.transaction.Transaction;
import js.transaction.TransactionException;
import js.transaction.TransactionManager;
import js.transaction.WorkingUnit;
import js.util.Classes;
import js.util.Strings;

/**
 * EclipseLink implementation for {@link TransactionManager} interface. Beside what is already stated on interface API,
 * note that 'transactional schema' supplied to transaction creation factory methods is named here 'persistence unit'.
 * Also persistence unit should be declared on persistence configuration file, '/META-INF/persistence.xml'.
 * <p>
 * This transaction manager implements support for multiple database schemas. Below is a piece of ASCII art depicting
 * components interaction.
 * <p>
 * User space Dao class methods are executed, after transaction creation, by ManagedProxyHandler from container. Dao
 * class is annotated with schema name. This schema name is detected by container and passed to transaction manager,
 * EclipseLink implementation - this class. Based on schema name, that in our case is actually persistence unit name, an
 * EntityManagerFactory is selected. Factories map is updated on the fly.
 * 
 * <pre>
 *       User Space Code
 *       +---------------------------------------+
 * +-----+-Transactional(schema)                 |
 * |     | class Dao {                           |
 * |     |    private final EntityManager em;    |
 * |     |    ...                                |
 * |  +--+--> method() {}                        |
 * |  |  | }                                     |
 * |  |  +---------------------------------------+
 * |  |  
 * |  |  Container                                                      EclipseLink j(s)lib transaction implementation    
 * |  |  +--------------------------------------------------------+     +--------------------------------------------------------+
 * |  |  | class ManagedProxyHandler {                            |     | class TransactionManagerImpl {                         | 
 * |  |  |    invoke(proxy) {                                     |     |       Map<String, EntityManagerFactory> factories;     |
 * |  |  |       ...                                              |     |                                                        | 
 * +--)--+-----> transactionManager.createTransaction(schema); ---+-----+-----> createTransaction(schema) {                      |
 *    |  |       ...                                              |     |          // here schema is the persistence unit name   |
 *    +--+-----  method.invoke();                                 |     |          // schema is used as factories key            |
 *       |       ...                                              |     |       }                                                |
 *       |    }                                                   |     |                                                        |
 *       | }                                                      |     | }                                                      |
 *       +--------------------------------------------------------+     +--------------------------------------------------------+
 * </pre>
 * 
 * @author Iulian Rotaru
 */
public class TransactionManagerImpl implements TransactionManager
{
  /** Class logger. */
  private static final Log log = LogFactory.getLog(TransactionManagerImpl.class);

  /** String pattern for persistence unit name. */
  private static final String PUNAME_MARK = "name=\"";

  /** Keep current on working transaction on thread local variable. */
  private final ThreadLocal<TransactionImpl> transactionsCache = new ThreadLocal<>();

  /**
   * By convention default persistence unit name is the first persistence unit declared in configuration file.
   */
  private String defaultPersistenceUnitName;

  /**
   * EntiTy manager factories, mapped per their persistence unit name. Default factory is initialized by
   * {@link #config(Config)} method with first persistence unit from configuration file. The other factories, if any are
   * declared on user space DAO classes, are added on the fly.
   */
  private final Map<String, EntityManagerFactory> factories = new HashMap<>();

  /**
   * Constructor used when transaction manager is instantiated by container. This constructor does nothing. Instance is
   * completely initialized after {@link #config(Config)}.
   */
  public TransactionManagerImpl()
  {
    log.trace("TransactionManagerImpl()");
  }

  /**
   * Constructor used when transaction manager instance is created by {@link TransactionContext}. Named persistence
   * unit name should exist into persistence configuration file.
   * 
   * @param persistenceUnitName persistence unit name.
   */
  public TransactionManagerImpl(String persistenceUnitName)
  {
    log.trace("TransactionManagerImpl(String)");
    defaultPersistenceUnitName = persistenceUnitName;
    factories.put(persistenceUnitName, Persistence.createEntityManagerFactory(persistenceUnitName));
  }

  @Override
  public void config(Config config) throws ConfigException
  {
    log.trace("config(Config)");
    // by convention persistence unit name is the schema name
    defaultPersistenceUnitName = getFirstPersistenceUnitName();
    factories.put(defaultPersistenceUnitName, Persistence.createEntityManagerFactory(defaultPersistenceUnitName));
  }

  @Override
  public Transaction createTransaction(String schema)
  {
    return createTransaction(schema, false);
  }

  @Override
  public Transaction createReadOnlyTransaction(String schema)
  {
    return createTransaction(schema, true);
  }

  /** Remove transaction from transactions cache. */
  void destroyTransaction()
  {
    transactionsCache.set(null);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <S, T> T exec(String persistenceUnitName, WorkingUnit<S, T> workingUnit, Object... args)
  {
    Transaction t = createTransaction(persistenceUnitName);
    T o = null;
    try {
      o = (T)workingUnit.exec((S)t.getResourceManager(), args);
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
  public <S, T> T exec(WorkingUnit<S, T> workingUnit, Object... args) throws TransactionException
  {
    return exec(null, workingUnit, args);
  }

  @Override
  public void destroy()
  {
    log.trace("destroy()");
    for(EntityManagerFactory factory : factories.values()) {
      factory.close();
    }
  }

  private TransactionImpl createTransaction(String persistenceUnitName, boolean readOnly)
  {
    TransactionImpl transaction = transactionsCache.get();
    if(transaction != null) {
      transaction.incrementTransactionNestingLevel();
      return transaction;
    }

    if(persistenceUnitName == null) {
      persistenceUnitName = defaultPersistenceUnitName;
    }

    EntityManagerFactory factory = factories.get(persistenceUnitName);
    if(factory == null) {
      // by convention persistence unit name is the schema name
      factory = Persistence.createEntityManagerFactory(persistenceUnitName);
      factories.put(persistenceUnitName, factory);
    }

    transaction = new TransactionImpl(this, factory.createEntityManager(), readOnly);
    transactionsCache.set(transaction);
    return transaction;
  }

  // ----------------------------------------------------------------------------------------------

  private static String getFirstPersistenceUnitName()
  {
    String persistence = null;
    try {
      persistence = Strings.load(Classes.getResourceAsReader("/META-INF/persistence.xml"));
    }
    catch(IOException e) {
      throw new BugError(e);
    }

    int start = persistence.indexOf(PUNAME_MARK);
    if(start == -1) {
      throw new BugError("Invalid persistence configuration. Missing persistence unit name.");
    }
    start += PUNAME_MARK.length();

    int end = persistence.indexOf('"', start);
    if(end == -1) {
      throw new BugError("Invalid persistence configuration. Missing persistence unit name.");
    }
    return persistence.substring(start, end);
  }
}
