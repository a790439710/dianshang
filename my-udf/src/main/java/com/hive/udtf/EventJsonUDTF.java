package com.hive.udtf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;

import java.util.ArrayList;
import java.util.List;

public class EventJsonUDTF extends GenericUDTF {

    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {

        List<String> fieldNames = new ArrayList<>();
        List<ObjectInspector> fieldTypes = new ArrayList<>();

        fieldNames.add("event_name");
        fieldTypes.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        fieldNames.add("event_json");
        fieldTypes.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldTypes);
    }

    @Override
    public void process(Object[] objects) throws HiveException {
        String str = objects[0].toString();
        try {
            JSONArray jsonArray = new JSONArray(str);
            for (int i=0; i< jsonArray.length(); i++){
                String[] result = new String[2];
                try {
                    result[0] = jsonArray.getJSONObject(i).getString("en");
                    result[1] = jsonArray.getJSONObject(i).toString();
                }catch (Exception e){
                    e.printStackTrace();
                    continue;
                }

                //forward将结果返回
                forward(result);
            }
        } catch (JSONException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
