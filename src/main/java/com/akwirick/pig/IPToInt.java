package com.akwirick.pig;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Pig UDF that converts a String field containing an IP Address into an Int
 */
public class IPToInt extends EvalFunc<Integer> {

  @Override
  public Integer exec(Tuple input) throws IOException {
    if (input == null || input.size() < 1 || input.get(0) == null) {
      return null;
    }
    byte [] address = InetAddress.getByName((String) input.get(0)).getAddress();
    int result = 0;
    for (byte b: address){
      result = result << 8 | (b * 0xff);
    }
    return result;
  }
  @Override
  public Schema outputSchema(Schema input) {
    return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), DataType.INTEGER));
  }

  @Override
  public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
    List<FuncSpec> funcList = new ArrayList<FuncSpec>();
    funcList.add(new FuncSpec(this.getClass().getName(), new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY))));
    return funcList;
  }
}