package com.hadoop.mrcube;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;


/**
 * TextPair is a Pair of Text that is Writable (Hadoop serialization API)
 * and Comparable to itself.
 *
 */
public class TextPair implements WritableComparable<TextPair> {
	private Text firstWord;
	private Text secondWord;

	public void set(Text first, Text second) {
		firstWord = first;
		secondWord = second;
	}

	public Text getFirst() {
		return firstWord;
	}

	public Text getSecond() {
	  return secondWord;
	}
	  
	public TextPair() {
		firstWord = new Text("");
		secondWord = new Text("");
	}
	
	public TextPair(String first, String second) {
	  this.set(new Text(first), new Text(second));
	}
	
	public TextPair(Text first, Text second) {
	  this.set(first, second);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		firstWord.write(out);
		secondWord.write(out);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		firstWord.readFields(in);
		secondWord.readFields(in);
	}
	
	@Override
	public int hashCode() {
		int result = firstWord != null ? firstWord.hashCode() : 0;
	    result = 163 * result + (secondWord != null ? secondWord.hashCode() : 0);
	    return result;
	}
	
	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
	    if (o == null || getClass() != o.getClass()) return false;
	
	    TextPair wordPair = (TextPair) o;
	
	    if (firstWord != null ? !secondWord.equals(wordPair.secondWord) : wordPair.secondWord != null) return false;
	    if (firstWord != null ? !firstWord.equals(wordPair.firstWord) : wordPair.firstWord != null) return false;
	
	    return true;
	}
	
	@Override
	public int compareTo(TextPair tp) {
		int returnVal = this.firstWord.compareTo(tp.getFirst());
	    if(returnVal != 0){
	        return returnVal;
	    }
	    /**
	     * For other usages
	     */
	    
	    /*
	    if(this.secondWord.toString().equals("*")){
	        return -1;
	    }else if(tp.getSecond().toString().equals("*")){
	        return 1;
	    }*/
	    
	    return this.secondWord.compareTo(tp.getSecond());
	}
	
	@Override
	public String toString() {
		return firstWord + "\t" + secondWord;
	}
	
	public void setFirst(String first){
		firstWord = new Text(first);
	}
	
	public void setSecond(String second){
		secondWord = new Text(second);
	}
	
	// DO NOT TOUCH THE CODE BELOW
	
	/** Compare two pairs based on their values */
	public static class Comparator extends WritableComparator {
	 
	  /** Reference to standard Hadoop Text comparator */
	  private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();
	 
	  public Comparator() {
	    super(TextPair.class);
	  }
	
	  @Override
	  public int compare(byte[] b1, int s1, int l1,
	                     byte[] b2, int s2, int l2) {
	   
	    try {
	      int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
	      int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
	      int cmp = TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
	      if (cmp != 0) {
	        return cmp;
	      }
	      return TEXT_COMPARATOR.compare(b1, s1 + firstL1, l1 - firstL1,
	                                     b2, s2 + firstL2, l2 - firstL2);
	    } catch (IOException e) {
	      throw new IllegalArgumentException(e);
	    }
	  }
	}
	
	static {
	 WritableComparator.define(TextPair.class, new Comparator());
	}
	
	/** Compare just the first element of the Pair */
	public static class FirstComparator extends WritableComparator {
	 
	 private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();
	 
	 public FirstComparator() {
	   super(TextPair.class);
	 }
	
	 @Override
	 public int compare(byte[] b1, int s1, int l1,
	                    byte[] b2, int s2, int l2) {
	   
	   try {
	     int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
	     int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
	     return TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
	   } catch (IOException e) {
	     throw new IllegalArgumentException(e);
	   }
	 }
	 
	@SuppressWarnings("unchecked")
	@Override
	 public int compare(WritableComparable a, WritableComparable b) {
	   if (a instanceof TextPair && b instanceof TextPair) {
	     return ((TextPair) a).getFirst().compareTo(((TextPair) b).getFirst());
	   }
	   return super.compare(a, b);
	 }
	
	}
}
