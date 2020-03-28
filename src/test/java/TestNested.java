import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.complex.NonNullableStructVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.ComplexWriterImpl;
import org.apache.arrow.vector.complex.impl.SingleStructReaderImpl;
import org.apache.arrow.vector.complex.impl.UnionListReader;
import org.apache.arrow.vector.complex.impl.VarCharWriterImpl;
import org.apache.arrow.vector.complex.reader.BaseReader;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.complex.writer.BigIntWriter;
import org.apache.arrow.vector.complex.writer.IntWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.Text;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestNested {

  private BufferAllocator allocator;
  private static final int NUM_COLUMN_VALUES = 100;

  @Before
  public void init() {
    allocator = new RootAllocator(Integer.MAX_VALUE);
  }

  @After
  public void terminate() throws Exception {
    allocator.close();
  }

  /**
   * STRUCT {
   *   age: INT
   *   salary: LONG
   * }
   */
  @Test
  public void testOneLevelStructHierarchy() {
    FieldType structFieldType = new FieldType(false, ArrowType.Struct.INSTANCE, null, null);
    String vectorName = "top-level-struct";
    String writerName = "root";
    StructVector structColumn = new StructVector(vectorName, allocator, structFieldType,null);

    // Write into STRUCT column
    BaseWriter.ComplexWriter complexObjectWriter = new ComplexWriterImpl(writerName, structColumn);
    // We will write a complex object as a struct at the root level
    BaseWriter.StructWriter rootStructWriter = complexObjectWriter.rootAsStruct();
    IntWriter ageWriter = rootStructWriter.integer("age");
    BigIntWriter salaryWriter = rootStructWriter.bigInt("salary");
    int ageStart = 5;
    long salaryStart = 50000;
    for (int i = 0; i < NUM_COLUMN_VALUES; i++) {
      rootStructWriter.start();
      ageWriter.writeInt(ageStart + i);
      salaryWriter.writeBigInt(salaryStart + 10000 + i);
      rootStructWriter.end();
    }

    // finalize the column data after writing the desired number of column values
    complexObjectWriter.setValueCount(NUM_COLUMN_VALUES);

    // Read from STRUCT column
    BaseReader.StructReader rootStructReader = new SingleStructReaderImpl(structColumn).reader("root");
    for (int i = 0; i < NUM_COLUMN_VALUES; i++) {
      rootStructReader.setPosition(i);
      Assert.assertEquals(ageStart + i, rootStructReader.reader("age").readInteger().intValue());
      Assert.assertEquals(salaryStart + 10000 + i, rootStructReader.reader("salary").readLong().longValue());
    }

    structColumn.close();
  }

  /**
   * STRUCT {
   *   age: INT
   *   salary: LONG
   *   address: {
   *     apt#: INT
   *     zip: INT
   *   }
   * }
   */
  @Test
  public void testMultiLevelStructHierarchy() {
    FieldType structFieldType = new FieldType(false, ArrowType.Struct.INSTANCE, null, null);
    String vectorName = "top-level-struct";
    String writerName = "root";
    StructVector structColumn = new StructVector(vectorName, allocator, structFieldType,null);

    // Write into STRUCT column
    BaseWriter.ComplexWriter complexObjectWriter = new ComplexWriterImpl(writerName, structColumn);
    // We will write a complex object as a struct at the root level
    BaseWriter.StructWriter rootStructWriter = complexObjectWriter.rootAsStruct();
    IntWriter ageWriter = rootStructWriter.integer("age");
    BigIntWriter salaryWriter = rootStructWriter.bigInt("salary");
    BaseWriter.StructWriter addressStructWriter = rootStructWriter.struct("address");
    IntWriter aptWriter = addressStructWriter.integer("apt");
    IntWriter zipWriter = addressStructWriter.integer("zip");

    // ROW 1
    rootStructWriter.start();
    ageWriter.writeInt(35);
    salaryWriter.writeBigInt(100000);
    addressStructWriter.start();
    aptWriter.writeInt(1016);
    zipWriter.writeInt(94404);
    addressStructWriter.end();
    rootStructWriter.end();

    // ROW 2
    rootStructWriter.start();
    ageWriter.writeInt(55);
    salaryWriter.writeBigInt(200000);
    addressStructWriter.start();
    aptWriter.writeInt(1013);
    zipWriter.writeInt(94402);
    addressStructWriter.end();
    rootStructWriter.end();

    // finalize the column data after writing the desired number of column values
    complexObjectWriter.setValueCount(2);

    // Read from STRUCT column
    BaseReader.StructReader rootStructReader = new SingleStructReaderImpl(structColumn).reader("root");
    BaseReader.StructReader addressStructReader = rootStructReader.reader("address");

    // Read ROW 0
    rootStructReader.setPosition(0);
    Assert.assertEquals(35, rootStructReader.reader("age").readInteger().intValue());
    Assert.assertEquals(100000, rootStructReader.reader("salary").readLong().longValue());
    Assert.assertEquals(1016, addressStructReader.reader("apt").readInteger().intValue());
    Assert.assertEquals(94404, addressStructReader.reader("zip").readInteger().intValue());

    // Read ROW 1
    rootStructReader.setPosition(1);
    Assert.assertEquals(55, rootStructReader.reader("age").readInteger().intValue());
    Assert.assertEquals(200000, rootStructReader.reader("salary").readLong().longValue());
    Assert.assertEquals(1013, addressStructReader.reader("apt").readInteger().intValue());
    Assert.assertEquals(94402, addressStructReader.reader("zip").readInteger().intValue());

    structColumn.close();
  }

  /**
   * STRUCT {
   *   age: INT
   *   salary: LONG
   *   list_addresses: {
   *     apt#: INT
   *     zip: INT
   *     list_phones: {
   *       number: INT
   *       type: INT
   *     }
   *   }
   * }
   */
  @Test
  public void testMultiLevelStructHierarchyWithList() {
    FieldType structFieldType = new FieldType(false, ArrowType.Struct.INSTANCE, null, null);
    String vectorName = "top-level-struct";
    String writerName = "root";
    StructVector structColumn = new StructVector(vectorName, allocator, structFieldType,null);

    // Write into STRUCT column
    BaseWriter.ComplexWriter complexObjectWriter = new ComplexWriterImpl(writerName, structColumn);
    // We will write a complex object as a struct at the root level
    BaseWriter.StructWriter rootStructWriter = complexObjectWriter.rootAsStruct();
    IntWriter ageWriter = rootStructWriter.integer("age");
    BigIntWriter salaryWriter = rootStructWriter.bigInt("salary");
    BaseWriter.ListWriter addressListWriter = rootStructWriter.list("addresses");
    BaseWriter.StructWriter addressStructWriter = addressListWriter.struct();
    IntWriter aptWriter = addressStructWriter.integer("apt");
    IntWriter zipWriter = addressStructWriter.integer("zip");
    BaseWriter.ListWriter phoneListWriter = addressStructWriter.list("phones");
    BaseWriter.StructWriter phoneStructWriter = phoneListWriter.struct();
    IntWriter numberWriter = phoneStructWriter.integer("phone-number");
    IntWriter typeWriter = phoneStructWriter.integer("phone-type");

    // INSERT 2 rows, 2 addresses per person and 3 phones per address

    // ROW 1
    rootStructWriter.start(); // start first column value (row) in parent struct
    ageWriter.writeInt(35);
    salaryWriter.writeBigInt(100000);

    addressListWriter.startList(); // start list of addresses

    addressStructWriter.start(); // start first address (struct) in the list
    aptWriter.writeInt(1016);
    zipWriter.writeInt(94404);

    phoneListWriter.startList(); // start list of 3 phones

    phoneStructWriter.start(); // start first phone (struct) in the list
    numberWriter.writeInt(412482);
    typeWriter.writeInt(0);
    phoneStructWriter.end();

    phoneStructWriter.start(); // start second phone (struct) in the list
    numberWriter.writeInt(412483);
    typeWriter.writeInt(1);
    phoneStructWriter.end();

    phoneStructWriter.start(); // start third phone (struct) in the list
    numberWriter.writeInt(412484);
    typeWriter.writeInt(2);
    phoneStructWriter.end();

    phoneListWriter.endList(); // end list of 3 phones

    addressStructWriter.end(); // end first address (struct) in the list

    addressStructWriter.start(); // start second address (struct) in the list
    aptWriter.writeInt(1017);
    zipWriter.writeInt(94405);

    phoneListWriter.startList(); // start list of 3 phones

    phoneStructWriter.start(); // start first phone (struct) in the list
    numberWriter.writeInt(412483);
    typeWriter.writeInt(0);
    phoneStructWriter.end();

    phoneStructWriter.start(); // start second phone (struct) in the list
    numberWriter.writeInt(412484);
    typeWriter.writeInt(1);
    phoneStructWriter.end();

    phoneStructWriter.start(); // start third phone (struct) in the list
    numberWriter.writeInt(412485);
    typeWriter.writeInt(2);
    phoneStructWriter.end();

    phoneListWriter.endList(); // end list of 3 phones
    addressStructWriter.end(); // end second address (struct) in the list

    addressListWriter.endList(); // end list of 2 addresses
    rootStructWriter.end();

    // ROW 2
    rootStructWriter.start(); // start second column value (row) in parent struct
    ageWriter.writeInt(55);
    salaryWriter.writeBigInt(200000);

    addressListWriter.startList(); // start list of addresses

    addressStructWriter.start(); // start first address (struct) in the list
    aptWriter.writeInt(1020);
    zipWriter.writeInt(94402);

    phoneListWriter.startList(); // start list of 3 phones

    phoneStructWriter.start(); // start first phone (struct) in the list
    numberWriter.writeInt(100);
    typeWriter.writeInt(0);
    phoneStructWriter.end();

    phoneStructWriter.start(); // start second phone (struct) in the list
    numberWriter.writeInt(101);
    typeWriter.writeInt(1);
    phoneStructWriter.end();

    phoneStructWriter.start(); // start third phone (struct) in the list
    numberWriter.writeInt(102);
    typeWriter.writeInt(2);
    phoneStructWriter.end();

    phoneListWriter.endList(); // end list of 3 phones

    addressStructWriter.end(); // end first address (struct) in the list

    addressStructWriter.start(); // start second address (struct) in the list
    aptWriter.writeInt(1021);
    zipWriter.writeInt(94403);

    phoneListWriter.startList(); // start list of 3 phones

    phoneStructWriter.start(); // start first phone (struct) in the list
    numberWriter.writeInt(101);
    typeWriter.writeInt(0);
    phoneStructWriter.end();

    phoneStructWriter.start(); // start second phone (struct) in the list
    numberWriter.writeInt(102);
    typeWriter.writeInt(1);
    phoneStructWriter.end();

    phoneStructWriter.start(); // start third phone (struct) in the list
    numberWriter.writeInt(103);
    typeWriter.writeInt(2);
    phoneStructWriter.end();

    phoneListWriter.endList(); // end list of 3 phones
    addressStructWriter.end(); // end second address (struct) in the list

    addressListWriter.endList(); // end list of 2 addresses
    rootStructWriter.end();

    // finalize the column data after writing the desired number of column values
    complexObjectWriter.setValueCount(2);

    // Read from STRUCT column
    BaseReader.StructReader rootStructReader = new SingleStructReaderImpl(structColumn).reader("root");
    UnionListReader addressListReader = (UnionListReader)rootStructReader.reader("addresses");
    BaseReader.StructReader addressStructReader = addressListReader.reader();
    UnionListReader phoneListReader = (UnionListReader)addressStructReader.reader("phones");
    BaseReader.StructReader phoneStructReader = phoneListReader.reader();

    // Read ROW 1
    rootStructReader.setPosition(0);
    Assert.assertEquals(35, rootStructReader.reader("age").readInteger().intValue());
    Assert.assertEquals(100000, rootStructReader.reader("salary").readLong().longValue());
    int i = 0;
    while (addressListReader.next()) {
      Assert.assertEquals(1016 + i, addressStructReader.reader("apt").readInteger().intValue());
      Assert.assertEquals(94404 + i, addressStructReader.reader("zip").readInteger().intValue());
      int j = 0;
      while (phoneListReader.next()) {
        Assert.assertEquals(412482 + i + j, phoneStructReader.reader("phone-number").readInteger().intValue());
        Assert.assertEquals(j, phoneStructReader.reader("phone-type").readInteger().intValue());
        j++;
      }
      i++;
    }

    // Read ROW 2
    rootStructReader.setPosition(1);
    Assert.assertEquals(55, rootStructReader.reader("age").readInteger().intValue());
    Assert.assertEquals(200000, rootStructReader.reader("salary").readLong().longValue());
    i = 0;
    while (addressListReader.next()) {
      Assert.assertEquals(1020 + i, addressStructReader.reader("apt").readInteger().intValue());
      Assert.assertEquals(94402 + i, addressStructReader.reader("zip").readInteger().intValue());
      int j = 0;
      while (phoneListReader.next()) {
        Assert.assertEquals(100 + i + j, phoneStructReader.reader("phone-number").readInteger().intValue());
        Assert.assertEquals(j, phoneStructReader.reader("phone-type").readInteger().intValue());
        j++;
      }
      i++;
    }

    structColumn.close();
  }
}
