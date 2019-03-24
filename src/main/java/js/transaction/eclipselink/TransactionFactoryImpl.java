package js.transaction.eclipselink;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import js.log.Log;
import js.log.LogFactory;
import js.transaction.Immutable;
import js.transaction.Mutable;
import js.transaction.Transaction;
import js.transaction.TransactionContext;
import js.transaction.TransactionFactory;
import js.transaction.TransactionManager;
import js.util.Classes;

public class TransactionFactoryImpl implements TransactionFactory, TransactionContext
{
  private static final Log log = LogFactory.getLog(TransactionFactoryImpl.class);

  private static final String DEF_PERSISTENCE_UNIT_NAME = "test";

  private final TransactionManager manager;
  private final ThreadLocal<Object> sessionStorage = new ThreadLocal<>();

  /**
   * Convenient constructor using default persistence unit name, <code>test</code>.
   */
  public TransactionFactoryImpl()
  {
    log.trace("TransactionFactoryImpl()");
    manager = new TransactionManagerImpl(DEF_PERSISTENCE_UNIT_NAME);
  }

  /**
   * Create transaction factory for named persistence unit.
   * 
   * @param persistenceUnitName persistence unit name.
   */
  public TransactionFactoryImpl(String persistenceUnitName)
  {
    log.trace("TransactionFactoryImpl(String)");
    manager = new TransactionManagerImpl(persistenceUnitName);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <I> I newInstance(Class<? extends I> implementationClass, Object... args)
  {
    log.trace("newInstance(Class<? extends I>, Object...)");
    ClassLoader classLoader = implementationClass.getClassLoader();
    Class<?>[] interfaces = implementationClass.getInterfaces();
    I instance = (I)Classes.newInstance(implementationClass, new EntityManagerImpl(this));
    InvocationHandler handler = new TransactionalProxy(instance);
    return (I)Proxy.newProxyInstance(classLoader, interfaces, handler);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T getSession()
  {
    return (T)sessionStorage.get();
  }

  // ----------------------------------------------------------------------------------------------
  // INNER CLASSES

  /**
   * Java invocation handler for transactional Proxy. Executes every method in transaction boundaries.
   * 
   * @author Iulian Rotaru
   * @version draft
   */
  private class TransactionalProxy implements InvocationHandler
  {
    private final Object instance;
    private final boolean immutableClass;

    public TransactionalProxy(Object instance)
    {
      this.instance = instance;

      Annotation annotation = instance.getClass().getAnnotation(Immutable.class);
      for(Class<?> interfaceClass : instance.getClass().getInterfaces()) {
        if(annotation != null) {
          break;
        }
        annotation = interfaceClass.getAnnotation(Immutable.class);
      }
      immutableClass = annotation != null;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable
    {
      boolean immutableMethod = immutableClass;
      if(getAnnotation(method, Immutable.class) != null) {
        immutableMethod = true;
      }
      if(getAnnotation(method, Mutable.class) != null) {
        immutableMethod = false;
      }

      Transaction transaction = immutableMethod ? manager.createReadOnlyTransaction() : manager.createTransaction();
      sessionStorage.set(transaction.getSession());

      try {
        Object value = method.invoke(instance, args);
        if(!immutableMethod) {
          transaction.commit();
        }
        return value;
      }
      catch(Throwable throwable) {
        if(!immutableMethod) {
          transaction.rollback();
        }
        throw throwable;
      }
      finally {
        transaction.close();
        sessionStorage.remove();
      }
    }

    private Annotation getAnnotation(Method interfaceMethod, Class<? extends Annotation> annotationClass) throws NoSuchMethodException, SecurityException
    {
      Annotation annotation = interfaceMethod.getAnnotation(annotationClass);
      if(annotation != null) {
        return annotation;
      }
      Class<?> implementationClass = instance.getClass();
      Method implementationMethod = implementationClass.getDeclaredMethod(interfaceMethod.getName(), interfaceMethod.getParameterTypes());
      return implementationMethod.getAnnotation(annotationClass);
    }
  }
}
