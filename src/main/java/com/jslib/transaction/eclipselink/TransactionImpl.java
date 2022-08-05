package com.jslib.transaction.eclipselink;

import com.jslib.api.transaction.Transaction;
import com.jslib.api.transaction.TransactionException;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityTransaction;

/**
 * EclipseLink implementation for {@link Transaction} interface.
 * 
 * @author Iulian Rotaru
 */
public class TransactionImpl implements Transaction
{
  /** Reference to parent transaction manager used to auto-destroy this transaction. */
  private final TransactionManagerImpl transactionManager;

  /** Reference to JPA entity manager used to create the underlying transaction, see {@link #transaction}. */
  private final EntityManager entityManager;

  /** Underlying JPA transaction. */
  private final EntityTransaction transaction;

  /**
   * Nesting level count used to allow for nesting transactions. {@link HibernateAdapter#createTransaction(boolean)}
   * increments this counter if a transaction is already present on current thread but avoid creating a new one. On
   * {@link #close()} decrement nesting level and perform the actual close only if nesting level is 0.
   */
  private int nestingLevel;

  /** A read only transaction does not explicitly begin or commit/rollback but rely on database (driver). */
  private final boolean readOnly;

  /** Flag indicating that transaction was closes and is not longer legal to operate on it. */
  private boolean closed = false;

  /**
   * Create transaction instance. If this transaction is not read only create the underlying JPA transaction. Otherwise
   * {@link #transaction} field remains null.
   * 
   * @param transactionManager parent transaction manager,
   * @param entityManager JPA entity manager,
   * @param readOnly flag true if need to create read only transaction.
   */
  public TransactionImpl(TransactionManagerImpl transactionManager, EntityManager entityManager, boolean readOnly)
  {
    this.transactionManager = transactionManager;
    this.entityManager = entityManager;
    this.readOnly = readOnly;

    // do not create transaction boundaries if session is read-only
    if(!readOnly) {
      try {
        this.transaction = this.entityManager.getTransaction();
        this.transaction.begin();
      }
      catch(Exception e) {
        if(this.entityManager.isOpen()) {
          this.entityManager.close();
        }
        throw new TransactionException(e);
      }
    }
    else {
      this.transaction = null;
    }
  }

  @Override
  public void commit()
  {
    if(nestingLevel > 0) {
      return;
    }
    if(readOnly) {
      throw new IllegalStateException("Read-only transaction does not allow commit.");
    }
    try {
      transaction.commit();
    }
    catch(Exception e) {
      throw new TransactionException(e);
    }
    finally {
      close();
    }
  }

  @Override
  public void rollback()
  {
    if(nestingLevel > 0) {
      return;
    }
    if(readOnly) {
      throw new IllegalStateException("Read-only transaction does not allow rollback.");
    }
    try {
      if(entityManager.isOpen()) {
        transaction.rollback();
      }
    }
    catch(Exception e) {
      throw new TransactionException(e);
    }
    finally {
      close();
    }
  }

  @Override
  public boolean close()
  {
    if(closed) {
      return true;
    }
    if(nestingLevel-- > 0) {
      return false;
    }
    closed = true;

    try {
      entityManager.close();
    }
    catch(Exception e) {
      throw new TransactionException(e);
    }
    finally {
      transactionManager.destroyTransaction();
    }
    return true;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T getResourceManager()
  {
    if(closed) {
      throw new IllegalStateException("Closed JPA entity manager.");
    }
    return (T)entityManager;
  }

  /**
   * Increment transaction nesting level. Invoked from {@link HibernateAdapter#createTransaction(boolean)}.
   * 
   * @see #nestingLevel
   */
  public void incrementTransactionNestingLevel()
  {
    nestingLevel++;
  }
}
