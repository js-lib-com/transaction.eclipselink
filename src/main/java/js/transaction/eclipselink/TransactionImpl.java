package js.transaction.eclipselink;

import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;

import js.transaction.Transaction;
import js.transaction.TransactionException;

public class TransactionImpl implements Transaction
{
  private final TransactionManagerImpl manager;

  private final EntityManager entity;

  private EntityTransaction transaction;

  /**
   * Nesting level count used to allow for nesting transactions. {@link HibernateAdapter#createTransaction(boolean)}
   * increments this counter if a transaction is already present on current thread but avoid creating a new one. On
   * {@link #close()} decrement nesting level and perform the actual close only if nesting level is 0.
   */
  private int nestingLevel;

  /** A read only transaction does not explicitly begin or commit/rollback but rely on database (driver). */
  private final boolean readOnly;

  /** Flag to detect if transaction working unit is actually using transactional session. */
  private boolean unused = true;

  /** Flag indicating that transaction was closes and is not longer legal to operate on it. */
  private boolean closed = false;

  public TransactionImpl(TransactionManagerImpl manager, boolean readOnly)
  {
    this.manager = manager;
    this.entity = manager.getEntityManager();
    this.readOnly = readOnly;

    // do not create transaction boundaries if session is read-only
    if(!readOnly) {
      try {
        this.transaction = this.entity.getTransaction();
        this.transaction.begin();
      }
      catch(Exception e) {
        if(this.entity.isOpen()) {
          this.entity.close();
        }
        throw new TransactionException(e);
      }
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
      if(entity.isOpen()) {
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
      entity.close();
    }
    catch(Exception e) {
      throw new TransactionException(e);
    }
    finally {
      manager.destroyTransaction();
    }
    return true;
  }

  @Override
  public boolean unused()
  {
    return unused;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T getSession()
  {
    if(closed) {
      throw new IllegalStateException("Closed Hibernate session.");
    }
    unused = false;
    return (T)entity;
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
