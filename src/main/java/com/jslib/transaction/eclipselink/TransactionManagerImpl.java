package com.jslib.transaction.eclipselink;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.jslib.api.log.Log;
import com.jslib.api.log.LogFactory;
import com.jslib.api.transaction.Transaction;
import com.jslib.api.transaction.TransactionException;
import com.jslib.api.transaction.TransactionManager;
import com.jslib.api.transaction.WorkingUnit;
import com.jslib.lang.BugError;
import com.jslib.util.Classes;
import com.jslib.util.Strings;

import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;

/**
 * EclipseLink implementation for {@link TransactionManager} interface. Beside what is already stated on interface API,
 * note that 'transactional schema' supplied to transaction creation factory methods is named here 'persistence unit'.
 * Also persistence unit should be declared on persistence configuration file, '/META-INF/persistence.xml'.
 * 
 * This transaction manager implements support for multiple database schemas. Below is a piece of ASCII art depicting
 * components interaction.
 * 
 * Application Dao class methods are executed, after transaction creation, by transaction service from container. Dao
 * class is annotated with schema name. This schema name is detected by container and passed to transaction manager,
 * EclipseLink implementation - this class. Based on schema name, that in our case is actually persistence unit name, an
 * EntityManagerFactory is selected. Factories map is updated on the fly.
 * 
 * <pre>
 *       Application
 *       +---------------------------------------+
 * +-----+-@Transactional(schema)                |
 * |     | class Dao {                           |
 * |     |    private final EntityManager em;    |
 * |     |    ...                                |
 * |  +--+--> method() {}                        |
 * |  |  | }                                     |
 * |  |  +---------------------------------------+
 * |  |  
 * |  |  Container                                                      EclipseLink j(s)lib transaction implementation    
 * |  |  +--------------------------------------------------------+     +--------------------------------------------------------+
 * |  |  | class TransactionService {                             |     | class TransactionManagerImpl {                         | 
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
  private static final Log log = LogFactory.getLog(TransactionManagerImpl.class);

  /**
   * By convention default persistence unit name is the first persistence unit declared in '/META-INF/persistence.xml'
   * file.
   */
  private final String defaultPersistenceUnitName;

  /** Keep on working transaction on thread local variable. */
  private final ThreadLocal<TransactionImpl> transactionsCache = new ThreadLocal<>();

  /** Cache for entity manager factories, mapped per their persistence unit name. It is loaded on the fly. */
  private final Map<String, EntityManagerFactory> factories = new HashMap<>();

  /** Initialize default persistence unit name, see {@link #getFirstPersistenceUnitName()}. */
  public TransactionManagerImpl()
  {
    log.trace("TransactionManagerImpl()");
    this.defaultPersistenceUnitName = getFirstPersistenceUnitName();
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
    transactionsCache.remove();
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
    factories.forEach((unitName, factory) -> {
      log.debug("Close entity manager factory for pesistence unit |%s|.", unitName);
      factory.close();
    });
  }

  /**
   * Create a transaction using the entity manager for requested persistence unit. Persistence unit is identified by
   * given persistence unit name; if null, uses {@link #defaultPersistenceUnitName}.
   * 
   * This method uses entity manager factory loaded from {@link #factories} cache; on cache miss create entity manager
   * factory on the fly.
   * 
   * @param persistenceUnitName persistence unit name or null if to use the default name,
   * @param readOnly flag true for read only transaction.
   * @return transaction implementation.
   */
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

  /** String pattern for persistence unit name. */
  private static final String PUNAME_MARK = "name=\"";

  /**
   * Scan persistence configuration file - '/META-INF/persistence.xml', for first persistence unit declaration and
   * return its name. Throws bug error if persistence configuration is missing or not well formed.
   * 
   * @return name of the first persistence unit, never null.
   * @throws BugError if persistence configuration is missing or not well formed.
   */
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
