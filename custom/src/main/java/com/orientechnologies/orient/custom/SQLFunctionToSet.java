package com.orientechnologies.orient.custom;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

public class SQLFunctionToSet extends OSQLFunctionAbstract {
  public static final String NAME = "toSet";

  public SQLFunctionToSet() {
    super(NAME, 0, Integer.MAX_VALUE);
  }

  @Override
  public Object execute(OIdentifiable iCurrentRecord, ODocument iCurrentResult, Object[] iFuncParams, OCommandContext iContext) {
    HashSet<Object> result = new HashSet<Object>();

    for (Object param : iFuncParams) {
      if (param != null) {
        if (param instanceof Collection)
          result.addAll((Collection) param);
        else if (param instanceof Map)
          result.addAll(((Map) param).values());
        else
          result.add(param);
      }
    }

    return result;
  }

  @Override
  public String getSyntax() {
    return "toSet( value|collection|map )";
  }
}
