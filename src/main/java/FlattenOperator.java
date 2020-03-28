import io.netty.buffer.ArrowBuf;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.arrow.flatbuf.Int;
import org.apache.arrow.flatbuf.RecordBatch;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;

public class FlattenOperator {

  /********************************** NOTES ***********************************
   *
   * PERSON (STRUCT column)
   * person = struct (name, age, salary, list(struct(apt, zip, list(struct(phone number, type)))))
   * {
   *   name: john
   *   age: 25
   *   salary: 100,000
   *   [
   *      {
   *        apt: 1012
   *        zip: 94404
   *        [
   *          {
   *            phone-number: 123456,
   *            type: mobile
   *          },
   *          {
   *            phone-number: 565676,
   *            type: work
   *          },
   *          {
   *            phone-number: 212123244,
   *            type: emergency
   *          }
   *        ]
   *      },
   *      {
   *        apt: 1022,
   *        zip: 94402,
   *        [
   *          {
   *            phone-number: 212121,
   *            type: mobile
   *          },
   *          {
   *            phone-number: 32322,
   *            type: work
   *          },
   *          {
   *            phone-number: 232424,
   *            type: landline
   *          }
   *        ]
   *      }
   *   ]
   * }
   *
   * FLATTEN() will be used to break down lists/arrays or structs into distinct rows.
   * We can use FLATTEN in a sub-query and then apply WHERE clause referring
   * to the flattened results in the outer query.
   *
   * If the query is only touching the root level flat types of the nested type,
   * then there is no need to use FLATTEN.
   *
   * Example: let's say the schema is
   *
   *    int_col
   *    long_col
   *    person
   *
   *    Query 1: SELECT FOO.person.age, FOO.person.salary, FOO.int_col
   *             FROM FOO
   *             WHERE Foo.person.age > 26 AND FOO.long_col < 10000
   *
   * The moment we want to touch person.addresses which is not a root-level
   * flat type, we need to use FLATTEN to unnest it.
   *
   *    Query 2: SELECT FLATTEN(person) FROM foo
   *
   *    Output of above query if table has exactly one record
   *
   *    NAME        AGE       SALARY      APT     ZIP       PHONE       PHONE_TYPE
   *    john        25        100000      1012    94404     123456      mobile
   *    john        25        100000      1012    94404     565676      work
   *    john        25        100000      1012    94404     212123244   emergency
   *    john        25        100000      1022    94402     212121      mobile
   *    john        25        100000      1022    94402     32322       work
   *    john        25        100000      1022    94402     232424      landline
   *
   *    Query 3: SELECT count(*) FROM
   *                (SELECT FLATTEN(person) flattened FROM Foo) T
   *             WHERE T.flattened.name = "john" AND T.flattened.zip = 94402
   */

  ColumnarRecordBatch flatten(String column, ValueVector vector) {
    if (vector.getMinorType() != Types.MinorType.LIST && vector.getMinorType() != Types.MinorType.STRUCT) {
      throw new UnsupportedOperationException("Flatten is not supported on primitive root types");
    }
    ColumnarRecordBatch outputRecordBatch = new ColumnarRecordBatch();
    Map<Integer, ChildrenAndParent> levelToNodes = new HashMap<>();
    buildLevelToNodesMap(vector, levelToNodes, 0, null);
    int maxLevel = Integer.MIN_VALUE;
    for (int level : levelToNodes.keySet()) {
      maxLevel = Math.max(maxLevel, level);
    }
    // level 0 -- name, age, salary
    // level 1 -- apt, zip
    // level 2 -- phone-number, phone-type
    // Flatten/unnest bottom-up from higher level to lower propagating the repetition
    // level at higher level to lower level
    int recordCount = -1;
    int currentLevel = maxLevel;
    while (true) {
      recordCount++;
      while (currentLevel >= maxLevel) {
        ChildrenAndParent childrenAndParent = levelToNodes.get(maxLevel);
        ValueVector parent = childrenAndParent.parent;
        List<ValueVector> children = childrenAndParent.children;
        if (parent instanceof ListVector) {
          ListVector listVector = (ListVector)parent;
          ArrowBuf offsets = listVector.getOffsetBuffer();
          int start = offsets.getInt(recordCount * 4);
          int end = offsets.getInt((recordCount + 1) * 4);
          int innerRecordCount = 0;
          for (int i = start; i < end; i++, innerRecordCount++) {
            for (ValueVector child : children) {
              Object value = child.getObject(i);
              ValueVector out = outputRecordBatch.getVector(child.getName());
              if (out == null) {
                out = getOutputVector(child);
                outputRecordBatch.addVector(child.getName(), child);
              }
              setValue(out, innerRecordCount, value);
            }
          }
        } else {
          // TODO: incomplete code
        }
      }
    }
    return outputRecordBatch;
  }

  private void setValue(ValueVector vector, int index, Object value) {
    switch (vector.getMinorType()) {
      case INT:
        IntVector intVector = (IntVector)vector;
        intVector.set(index, (Integer)value);
        break;
      case BIGINT:
        BigIntVector bigIntVector = (BigIntVector)vector;
        bigIntVector.set(index, (Long)value);
        break;
      case FLOAT4:
        Float4Vector floatVector = (Float4Vector)vector;
        floatVector.set(index, (Float)value);
        break;
      case FLOAT8:
        Float8Vector doubleVector = (Float8Vector)vector;
        doubleVector.set(index, (Double)value);
        break;
      default:
        throw new IllegalStateException("output vectors are leaves which should be primitive type");
    }
  }

  private ValueVector getOutputVector(ValueVector inputVector) {
    switch (inputVector.getMinorType()) {
      case INT:
        return new IntVector(inputVector.getName(), inputVector.getAllocator());
      case BIGINT:
        return new BigIntVector(inputVector.getName(), inputVector.getAllocator());
      case FLOAT4:
        return new Float4Vector(inputVector.getName(), inputVector.getAllocator());
      case FLOAT8:
        return new Float8Vector(inputVector.getName(), inputVector.getAllocator());
      default:
        throw new IllegalStateException("output vectors are leaves which should be primitive type");
    }
  }

  private static class ChildrenAndParent {
    ValueVector parent;
    List<ValueVector> children;
    ChildrenAndParent(ValueVector parent) {
      this.parent = parent;
      this.children = new ArrayList<>();
    }
  }

  // person = struct (name, age, salary, list(struct(apt, zip, list(struct(phone number, type)))))
  private void buildLevelToNodesMap(
      ValueVector vector,
      Map<Integer, ChildrenAndParent> levelToNodes,
      int level,
      ValueVector parent) {
    switch (vector.getMinorType()) {
      case LIST:
        ListVector listVector = (ListVector)vector;
        // increase the level for the underlying data source of list
        // this will help us backtrack with repetition level for the inner count
        buildLevelToNodesMap(listVector.getDataVector(), levelToNodes, level + 1, listVector);
        break;
      case STRUCT:
        StructVector structVector = (StructVector)vector;
        for (String child : structVector.getChildFieldNames()) {
          FieldVector childVector = structVector.getChild(child);
          // this is buggy -- passing struct as parent won't help
          // we need to pass parent of struct (which for our example of person
          // happens to be list). Generalize this.
          // no need to change the level for children of struct since that
          // doesn't increase the repetition level
          buildLevelToNodesMap(childVector, levelToNodes, level, parent);
        }
        break;
      case INT:
      case BIGINT:
      case FLOAT4:
      case FLOAT8:
        ChildrenAndParent childrenAndParent = levelToNodes.get(level);
        if (childrenAndParent == null) {
          childrenAndParent = new ChildrenAndParent(parent);
          levelToNodes.put(level, childrenAndParent);
        }
        childrenAndParent.children.add(vector);
        break;
    }
  }

  public static class ColumnarRecordBatch {
    private final Map<String, ValueVector> columnData;
    private final Map<String, Types.MinorType> columnTypes;

    ColumnarRecordBatch () {
      columnData = new HashMap<>();
      columnTypes = new HashMap<>();
    }

    public void addVector(String column, ValueVector vector) {
      columnData.put(column, vector);
      columnTypes.put(column, vector.getMinorType());
    }

    ValueVector getVector(String column) {
      return  columnData.get(column);
    }

    Types.MinorType getMinorType(String column) {
      return columnTypes.get(column);
    }

    Set<String> getColumns() {
      return columnData.keySet();
    }
  }

  /*************************** UNUSED PARTIAL CODE ***********************************/

  private void flattenStruct(
      String column,
      ValueVector vector,
      ColumnarRecordBatch outputRecordBatch,
      String parentColumn,
      ValueVector parentVector) {
    Preconditions.checkArgument(vector instanceof StructVector);
    StructVector structVector = (StructVector)vector;
    for (String child : structVector.getChildFieldNames()) {
      FieldVector childVector = structVector.getChild(child);
      Types.MinorType childType = childVector.getMinorType();
      switch (childType) {
        case INT:
          IntVector intVector = new IntVector(child, structVector.getAllocator());
          outputRecordBatch.addVector(child, intVector);
          break;
        case BIGINT:
          BigIntVector bigIntVector = new BigIntVector(child, structVector.getAllocator());
          outputRecordBatch.addVector(child, bigIntVector);
          break;
        case FLOAT4:
          Float4Vector floatVector = new Float4Vector(child, structVector.getAllocator());
          outputRecordBatch.addVector(child, floatVector);
          break;
        case FLOAT8:
          Float8Vector doubleVector = new Float8Vector(child, structVector.getAllocator());
          outputRecordBatch.addVector(child, doubleVector);
          break;
      }
    }
  }

  private void flattenList(
      String column,
      ValueVector vector,
      ColumnarRecordBatch outputRecordBatch) {
    Preconditions.checkArgument(vector instanceof ListVector);
    ListVector listVector = (ListVector)vector;
    FieldVector underlyingDataVector = listVector.getDataVector();
    Types.MinorType innerType = underlyingDataVector.getMinorType();
    switch (innerType) {
      case LIST:
        flattenList(underlyingDataVector.getName(), underlyingDataVector, outputRecordBatch);
      case STRUCT:
        flattenStruct(underlyingDataVector.getName(), underlyingDataVector, outputRecordBatch);
      case INT:
      case BIGINT:
      case FLOAT4:
      case FLOAT8:
        handleLeaves(underlyingDataVector.getName(), underlyingDataVector, outputRecordBatch, listVector.getName(), listVector);
    }
  }
}
