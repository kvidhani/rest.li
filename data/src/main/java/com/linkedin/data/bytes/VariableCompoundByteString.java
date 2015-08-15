package com.linkedin.data.bytes;


import com.linkedin.data.ByteString;
import com.linkedin.util.ArgumentUtil;
import java.util.ArrayList;
import java.util.List;


/**
 * Created by kvidhani on 8/14/15.
 */
public final class VariableCompoundByteString extends CompoundByteString
{
  private VariableCompoundByteString(final int byteOffsetBeginning,
      final List<ByteString> byteStringList) {
    super(byteOffsetBeginning, byteStringList);
  }

  private VariableCompoundByteString() {
    super(0, 0, new ArrayList<ByteString>());
  }

  public static VariableCompoundByteString create(final int byteOffsetBeginning, final List<ByteString> byteStringList) {
    ArgumentUtil.notNull(byteStringList, "Null ByteString list not allowed");
    return new VariableCompoundByteString(byteOffsetBeginning, byteStringList);
  }

  public static VariableCompoundByteString create() {
    return new VariableCompoundByteString();
  }

  //Add and return a new CompoundByteString
  public VariableCompoundByteString addAndCreateNew(final ByteString byteString) {
    if (byteString == null) {
      throw new IllegalArgumentException("Null ByteString provided");
    }

    return new VariableCompoundByteString(_byteOffsetBeginning, _byteStringList);
  }

  public VariableCompoundByteString trimAndCreateNew(final int newStartIndex) {

    if (newStartIndex < 0) {
      throw new IllegalArgumentException("Invalid new start index specified");
    }

    //We should always have this invariant true for this class
    assert(_byteOffsetEnd == _byteStringList.get(_byteStringList.size() -1).length());
    int currentCount = 0;
    final List<ByteString> newList = new ArrayList<ByteString>(_byteStringList);
    for (int i = 0; i < _byteStringList.size(); i++) {
      //Take into account the offset in the first and final ByteString
      int byteStringStart = i == 0 ? _byteOffsetBeginning : 0;
      for (int j = byteStringStart; j < _byteStringList.get(i).length(); j++) {
        currentCount++;
        if (currentCount == newStartIndex) {
          //We have found the index in our ByteString buffer where we want to trim up until.
          for (int k = 0; k < i; k++) {
            newList.remove(k);
          }
          return new VariableCompoundByteString(j, newList);
        }
      }
    }

    //We use this technique of calculating the limit instead of using size() to save an extra
    //iteration through all of our data.
    throw new IllegalArgumentException("Provided new start index larger then size of ByteString buffer");
    //todo test where you trim the whole list
  }

}
