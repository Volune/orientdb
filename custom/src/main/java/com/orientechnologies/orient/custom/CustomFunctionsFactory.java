package com.orientechnologies.orient.custom;

import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.functions.OSQLFunction;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionFactory;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class CustomFunctionsFactory implements OSQLFunctionFactory {

  private static final Map<String, Object> FUNCTIONS = new HashMap<String, Object>();

  static {
    FUNCTIONS.put(SQLFunctionFromStack.NAME.toUpperCase(Locale.ENGLISH), new SQLFunctionFromStack());
    FUNCTIONS.put(SQLFunctionToSet.NAME.toUpperCase(Locale.ENGLISH), new SQLFunctionToSet());
  }

  public Set<String> getFunctionNames() {
    return FUNCTIONS.keySet();
  }

  public boolean hasFunction(final String name) {
    return FUNCTIONS.containsKey(name);
  }

  public OSQLFunction createFunction(final String name) {
    final Object obj = FUNCTIONS.get(name);

    if (obj == null)
      throw new OCommandExecutionException("Unknowned function name :" + name);

    if (obj instanceof OSQLFunction)
      return (OSQLFunction) obj;
    else {
      // it's a class
      final Class<?> clazz = (Class<?>) obj;
      try {
        return (OSQLFunction) clazz.newInstance();
      } catch (Exception e) {
        throw new OCommandExecutionException("Error in creation of function " + name
            + "(). Probably there is not an empty constructor or the constructor generates errors", e);
      }
    }

  }
}
