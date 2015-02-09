package com.facebook.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Description(name = "udfremovefromarray",
        value = "_FUNC_(VALUES, VALUES_TO_REMOVE)",
        extended = "Example:\n"
                + "  > SELECT REMOVE_FROM_ARRAY(foo, array('remove_this')) FROM users;\n")
public class UDFRemoveFromArray extends GenericUDF {
    ListObjectInspector arrayOI = null;
    ObjectInspectorConverters.Converter converters[];

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments)
            throws UDFArgumentException {
        converters = new ObjectInspectorConverters.Converter[arguments.length];

        for (int ii = 0; ii < arguments.length; ++ii) {
            if (ii == 0) {
                arrayOI = (ListObjectInspector) ObjectInspectorUtils
                        .getStandardObjectInspector(arguments[ii]);
            }
            converters[ii] = ObjectInspectorConverters.getConverter(arguments[ii], arrayOI);
        }
        return arrayOI;
    }

    @Override
    public Object evaluate(GenericUDF.DeferredObject[] arguments) throws HiveException {
        List<Object> values = (List<Object>)converters[0].convert(arguments[0].get());
        List<Object> valuesToRemove = (List<Object>)converters[1].convert(arguments[1].get());

        ArrayList<Object> result_array = new ArrayList<Object>(values.size());

        if (values == null || valuesToRemove == null) {
            return null;
        }

        Set<Object> valuesToRemoveSet = new HashSet<Object>();
        valuesToRemoveSet.addAll(valuesToRemove);
        for( Object val : values ) {
            if (val != null && !valuesToRemoveSet.contains(val)) {
                result_array.add(val);
            }
        }

        return new ArrayList<Object>(result_array);
    }

    @Override
    public String getDisplayString(String[] children) {
        return new String();
    }
}