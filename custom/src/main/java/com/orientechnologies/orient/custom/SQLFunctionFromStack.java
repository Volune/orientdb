package com.orientechnologies.orient.custom;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.traverse.OTraverseAbstractProcess;
import com.orientechnologies.orient.core.command.traverse.OTraverseRecordProcess;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;

import java.util.ArrayList;
import java.util.List;

public class SQLFunctionFromStack extends OSQLFunctionAbstract {
  public static final String NAME = "fromStack";

  public SQLFunctionFromStack() {
    super(NAME, 0, 2);
  }

  @Override
  public Object execute(OIdentifiable iCurrentRecord, ODocument iCurrentResult, Object[] iFuncParams, OCommandContext iContext) {
    final List<OTraverseAbstractProcess<?>> stack = (List<OTraverseAbstractProcess<?>>) iContext.getVariable("stack");
    if (null == stack)
      return null;

    List<ODocument> documents = new ArrayList<ODocument>();
    for (OTraverseAbstractProcess element : stack) {
      if (element instanceof OTraverseRecordProcess)
        documents.add(((OTraverseRecordProcess) element).getTarget());
    }

    if (iFuncParams.length == 0) {
      return documents;
    } else if (iFuncParams.length == 1) {
      int index = ((Number) iFuncParams[0]).intValue();
      if (index < 0)
        index = documents.size() + index;
      if (index >= 0 && index < documents.size()) {
        return documents.get(index);
      } else
        return null;
    } else if (iFuncParams.length == 2) {
      int from = ((Number) iFuncParams[0]).intValue();
      if (from < 0) {
        from = documents.size() + from;
        if (from < 0)
          from = 0;
      }

      int to = ((Number) iFuncParams[1]).intValue();
      if (to < 0)
        to = documents.size() + to + 1;
      else if (to > documents.size())
        to = documents.size();

      if (from >= 0 && from < documents.size() && to >= 0 && to <= documents.size() && from < to) {
        return documents.subList(from, to);
      } else
        return null;

    } else
      return null; //TODO throw exception ?
  }

  @Override
  public String getSyntax() {
    return "fromStack( [from|index], [to] )";
  }
}
