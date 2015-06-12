package com.linkedin.multipart;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.linkedin.data.ByteString;
import com.linkedin.r2.filter.R2Constants;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static com.linkedin.multipart.utils.MIMETestUtils.*;
import static com.linkedin.multipart.utils.MIMETestUtils._purelyEmptyBody;

/**
 * Created by kvidhani on 8/9/15.
 */
//* Note that some of these tests will fail when run in Intellij until this is fixed:
//        * http://youtrack.jetbrains.com/issue/IDEA-102461#u=1401303768694

public class TestByteStringBuffer {

  private static final ByteString simpleByteString = ByteString.copy("abc".getBytes());
  private static final ByteString simpleByteString2 = ByteString.copy("def".getBytes());
  private static final ByteString simpleByteString3 = ByteString.copy("ghi".getBytes());
  private static final ByteString simpleByteString4 = ByteString.copy("1234".getBytes());


  //For our data sources, we make copies each time so we don't end up using the same list over and over again. In production
  //this should never happen because we always make sure to create new lists when creating ByteStringBuffer/ImmutableByteStringBuffer
  @DataProvider(name = "byteStringBufferWithOffsets")
  public Object[][] byteStringBufferWithOffsets() throws Exception
  {
    //We specify the original buffer, the offsets, and the expected size.
    return new Object[][]{

            {Lists.newArrayList(simpleByteString, simpleByteString2, simpleByteString3), 0, 3, 9},
            {Lists.newArrayList(simpleByteString, simpleByteString2, simpleByteString3), 0, 0, 6},
            {Lists.newArrayList(simpleByteString, simpleByteString2, simpleByteString3), 3, 0, 3},
            {Lists.newArrayList(simpleByteString, simpleByteString2, simpleByteString3), 3, 3, 6},

            {Lists.newArrayList(ByteString.empty(), simpleByteString), 0, 0, 0},
            {Lists.newArrayList(ByteString.empty(), simpleByteString), 0, 3, 3},
            {Lists.newArrayList(ByteString.empty(), simpleByteString), 0, 2, 2},
            {Lists.newArrayList(ByteString.empty(), simpleByteString, ByteString.empty()), 0, 0, 3},
            {Lists.newArrayList(simpleByteString, ByteString.empty()), 3, 0, 0},
            {Lists.newArrayList(simpleByteString, ByteString.empty()), 2, 0, 1},
            {Lists.newArrayList(simpleByteString, ByteString.empty()), 0, 0, 3},
    };
  }

  @Test(dataProvider = "byteStringBufferWithOffsets")
  public void testBufferSizeWithVariousOffsets(final List<ByteString> byteStringList,
                                               final int beginningOffset, final int endOffset, final int expectedSize)
  {
    ByteStringBuffer byteStringBuffer = new ByteStringBuffer(beginningOffset, endOffset, byteStringList);
    Assert.assertEquals(byteStringBuffer.size(), expectedSize);
  }

  @DataProvider(name = "byteStringBufferSizeWithAddition")
  public Object[][] byteStringBufferSizeWithAddition() throws Exception
  {
    return new Object[][]{

            //We specify:
            //1. The original buffer
            //2. The beginning and ending offsets upon creation
            //3. What is to be added
            //4. The expected resulting size and the expected resulting offsets.

            //Mutations are only allowed on the super class ByteStringBuffer and the invariant is that
            //the upper offset will always be equal to the last ByteString's length.
            //The beginning offset can vary due to trims().
            {Lists.newArrayList(simpleByteString, simpleByteString2, simpleByteString3), 0, 3, simpleByteString4, 13, 0, 4},
            {Lists.newArrayList(simpleByteString, simpleByteString2, simpleByteString3), 3, 3, simpleByteString4, 10, 3, 4},

            {Lists.newArrayList(simpleByteString, simpleByteString2, simpleByteString3), 0, 3, ByteString.empty(), 9, 0, 0},
            {Lists.newArrayList(simpleByteString, simpleByteString2, simpleByteString3), 3, 3, ByteString.empty(), 6, 3, 0},

            {Lists.newArrayList(ByteString.empty(), simpleByteString), 0, 3, simpleByteString4, 7, 0, 4},
            {Lists.newArrayList(ByteString.empty(), simpleByteString, ByteString.empty()), 0, 0, simpleByteString4, 7, 0, 4},
            {Lists.newArrayList(simpleByteString, ByteString.empty()), 3, 0, simpleByteString4, 4, 3, 4},
            {Lists.newArrayList(simpleByteString, ByteString.empty()), 2, 0, simpleByteString4, 5, 2, 4},
            {Lists.newArrayList(simpleByteString, ByteString.empty()), 0, 0, simpleByteString4, 7, 0, 4},

            {Lists.newArrayList(ByteString.empty(), simpleByteString), 0, 3, ByteString.empty(), 3, 0, 0},
            {Lists.newArrayList(ByteString.empty(), simpleByteString, ByteString.empty()), 0, 0, ByteString.empty(), 3, 0, 0},
            {Lists.newArrayList(simpleByteString, ByteString.empty()), 3, 0, ByteString.empty(), 0, 3, 0},
            {Lists.newArrayList(simpleByteString, ByteString.empty()), 2, 0, ByteString.empty(), 1, 2, 0},
            {Lists.newArrayList(simpleByteString, ByteString.empty()), 0, 0, ByteString.empty(), 3, 0, 0},
    };
  }

  @Test(dataProvider = "byteStringBufferSizeWithAddition")
  public void testAdditionAndSize(final List<ByteString> byteStringList,
                           final int beginningOffset, final int endOffset,
                           final ByteString byteStringToAdd,
                           final int expectedSize,
                           final int expectedBeginningOffset,
                           final int expectedEndOffset)
  {
    ByteStringBuffer byteStringBuffer = new ByteStringBuffer(beginningOffset, endOffset, byteStringList);
    byteStringBuffer.add(byteStringToAdd);
    Assert.assertEquals(byteStringBuffer.size(), expectedSize);
    Assert.assertEquals(byteStringBuffer._byteOffsetBeginning, expectedBeginningOffset);
    Assert.assertEquals(byteStringBuffer._byteOffsetEnd, expectedEndOffset);
  }


  @DataProvider(name = "byteStringBufferSizeWithRemoval")
  public Object[][] byteStringBufferSizeWithRemoval() throws Exception
  {
    return new Object[][]{

            //We specify:
            //1. The original buffer
            //2. The beginning and ending offsets upon creation
            //3. What is to be added
            //4. The expected resulting size and the expected resulting offsets.

            //Mutations are only allowed on the super class ByteStringBuffer and the invariant is that
            //the upper offset will always be equal to the last ByteString's length.
            //The beginning offset can vary due to trims().
            {Lists.newArrayList(simpleByteString, simpleByteString2, simpleByteString3), 0, 3, simpleByteString4, 13, 0, 4},
            {Lists.newArrayList(simpleByteString, simpleByteString2, simpleByteString3), 3, 3, simpleByteString4, 10, 3, 4},

            {Lists.newArrayList(simpleByteString, simpleByteString2, simpleByteString3), 0, 3, ByteString.empty(), 9, 0, 0},
            {Lists.newArrayList(simpleByteString, simpleByteString2, simpleByteString3), 3, 3, ByteString.empty(), 6, 3, 0},

            {Lists.newArrayList(ByteString.empty(), simpleByteString), 0, 3, simpleByteString4, 7, 0, 4},
            {Lists.newArrayList(ByteString.empty(), simpleByteString, ByteString.empty()), 0, 0, simpleByteString4, 7, 0, 4},
            {Lists.newArrayList(simpleByteString, ByteString.empty()), 3, 0, simpleByteString4, 4, 3, 4},
            {Lists.newArrayList(simpleByteString, ByteString.empty()), 2, 0, simpleByteString4, 5, 2, 4},
            {Lists.newArrayList(simpleByteString, ByteString.empty()), 0, 0, simpleByteString4, 7, 0, 4},

            {Lists.newArrayList(ByteString.empty(), simpleByteString), 0, 3, ByteString.empty(), 3, 0, 0},
            {Lists.newArrayList(ByteString.empty(), simpleByteString, ByteString.empty()), 0, 0, ByteString.empty(), 3, 0, 0},
            {Lists.newArrayList(simpleByteString, ByteString.empty()), 3, 0, ByteString.empty(), 0, 3, 0},
            {Lists.newArrayList(simpleByteString, ByteString.empty()), 2, 0, ByteString.empty(), 1, 2, 0},
            {Lists.newArrayList(simpleByteString, ByteString.empty()), 0, 0, ByteString.empty(), 3, 0, 0},
    };
  }

  @Test(dataProvider = "byteStringBufferSizeWithRemoval")
  public void testRemovalAndSize(final List<ByteString> byteStringList,
                                  final int beginningOffset, final int endOffset,
                                  final ByteString byteStringToRemove,
                                  final int expectedSize,
                                  final int expectedBeginningOffset,
                                  final int expectedEndOffset)
  {
    ByteStringBuffer byteStringBuffer = new ByteStringBuffer(beginningOffset, endOffset, byteStringList);
    //byteStringBuffer.add(byteStringToAdd);
    Assert.assertEquals(byteStringBuffer.size(), expectedSize);
    Assert.assertEquals(byteStringBuffer._byteOffsetBeginning, expectedBeginningOffset);
    Assert.assertEquals(byteStringBuffer._byteOffsetEnd, expectedEndOffset);


  }

  @Test
  public void tempTest() {
    Foo temp = new Bar();
    temp.printThis();

  }

  class Foo {
    int a;
    Foo() {

    }

    void printThis() {
      System.out.println("this");
    }
  }

  class Bar extends Foo {
    Bar() {
      super();
    }

    @Override
    void printThis() {
      System.out.println("that");
    }
  }

  //todo remove

//  byteStringBuffer.add(ByteString.copy("12345".getBytes()));
//  byteStringBuffer.add(ByteString.empty());
//  Assert.assertEquals(byteStringBuffer.size(), expectedSize + 5);
//remova


  //test 0,0  length, length, and cases where buffer is size 1
  //index of tests crossing byte strings and all sorts of cases

  @Test
  public void testIndexOf() {
    final ByteString simpleByteString = ByteString.copy("abc".getBytes());
    final ByteString simpleByteString2 = ByteString.copy("def".getBytes());
    final ByteString simpleByteString3 = ByteString.copy("ghi".getBytes());
    ByteStringBuffer byteStringBuffer = new ByteStringBuffer(0, 3, ImmutableList.of(simpleByteString, simpleByteString2, simpleByteString3));

    byteStringBuffer.indexOfBytes("abcd".getBytes());
  }
}
