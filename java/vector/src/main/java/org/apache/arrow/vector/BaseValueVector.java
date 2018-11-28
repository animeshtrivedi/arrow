/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.vector;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.arrow.memory.BaseAllocator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.util.OversizedAllocationException;
import org.apache.arrow.vector.util.TransferPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ArrowBuf;

public abstract class BaseValueVector implements ValueVector {
  private static final Logger logger = LoggerFactory.getLogger(BaseValueVector.class);

  public static final String MAX_ALLOCATION_SIZE_PROPERTY = "arrow.vector.max_allocation_bytes";
  public static final int MAX_ALLOCATION_SIZE = Integer.getInteger(MAX_ALLOCATION_SIZE_PROPERTY, Integer.MAX_VALUE);
  public static final int INITIAL_VALUE_ALLOCATION = 4096;

  protected final BufferAllocator allocator;
  protected final String name;

  private ArrowBuf validityBuffer;
  private int validityAllocationSizeInBytes;
  private int nullCount;

  protected BaseValueVector(String name, BufferAllocator allocator) {
    this.allocator = Preconditions.checkNotNull(allocator, "allocator cannot be null");
    this.name = name;
    this.validityBuffer = this.allocator.getEmpty();
    this.validityAllocationSizeInBytes = getValidityBufferSizeFromCount(INITIAL_VALUE_ALLOCATION);
  }

  public void loadValidityBuffer(final ArrowFieldNode fieldNode,
                                 final ArrowBuf bitBuffer,
                                 final BufferAllocator allocator){
    this.validityBuffer.release();
    this.validityBuffer = BitVectorHelper.loadValidityBuffer(fieldNode, bitBuffer, allocator);
    validityAllocationSizeInBytes = validityBuffer.capacity();
    this.nullCount = 0;
  }

  public int getValidityBufferCapacity(){
    return this.validityBuffer.capacity();
  }

  public void add(List<ArrowBuf> list){
    list.add(this.validityBuffer);
  }

  public void add(ArrowBuf[] buffers, int index){
    buffers[index] = this.validityBuffer;
  }

  public void setReaderIndex(int index){
    this.validityBuffer.readerIndex(index);
  }

  public void setWriterIndex(int index){
    this.validityBuffer.writerIndex(index);
  }

  final public void allocateValidityBuffer(final long size) {
    final int curSize = (int) size;
    validityBuffer = allocator.buffer(curSize);
    validityBuffer.readerIndex(0);
    validityAllocationSizeInBytes = curSize;
    validityBuffer.setZero(0, validityBuffer.capacity());
  }

  final public void reallocValidityBuffer() {
    final int currentBufferCapacity = validityBuffer.capacity();
    long baseSize = validityAllocationSizeInBytes;

    if (baseSize < (long) currentBufferCapacity) {
      baseSize = (long) currentBufferCapacity;
    }

    long newAllocationSize = baseSize * 2L;
    newAllocationSize = BaseAllocator.nextPowerOfTwo(newAllocationSize);
    assert newAllocationSize >= 1;

    if (newAllocationSize > MAX_ALLOCATION_SIZE) {
      throw new OversizedAllocationException("Unable to expand the buffer");
    }

    final ArrowBuf newBuf = allocator.buffer((int) newAllocationSize);
    newBuf.setBytes(0, validityBuffer, 0, currentBufferCapacity);
    newBuf.setZero(currentBufferCapacity, newBuf.capacity() - currentBufferCapacity);
    validityBuffer.release(1);
    validityBuffer = newBuf;
    validityAllocationSizeInBytes = (int) newAllocationSize;
  }

  public void setValidityAllocationSizeInBytes(int size){
    this.validityAllocationSizeInBytes = size;
  }

  public int getValidityAllocationSizeInBytes(){
    return this.validityAllocationSizeInBytes;
  }

  public void setValidityBuffer(ArrowBuf validityBuffer){
    this.validityBuffer = validityBuffer;
  }

  public ArrowBuf.TransferResult transferOwnership(BufferAllocator newAllocator){
    return validityBuffer.transferOwnership(newAllocator);
  }

  /**
   * Validity buffer has multiple cases of split and transfer depending on
   * the starting position of the source index.
   */
  final public void splitAndTransferValidityBuffer(int startIndex, int length, int valueCount,
                                              BaseValueVector target) {
    assert startIndex + length <= valueCount;
    int firstByteSource = BitVectorHelper.byteIndex(startIndex);
    int lastByteSource = BitVectorHelper.byteIndex(valueCount - 1);
    int byteSizeTarget = getValidityBufferSizeFromCount(length);
    int offset = startIndex % 8;

    if (length > 0) {
      if (offset == 0) {
        /* slice */
        if (target.validityBuffer != null) {
          target.validityBuffer.release();
        }
        target.validityBuffer = validityBuffer.slice(firstByteSource, byteSizeTarget);
        target.validityBuffer.retain(1);
      } else {
        /* Copy data
         * When the first bit starts from the middle of a byte (offset != 0),
         * copy data from src BitVector.
         * Each byte in the target is composed by a part in i-th byte,
         * another part in (i+1)-th byte.
         */
        target.allocateValidityBuffer(byteSizeTarget);

        for (int i = 0; i < byteSizeTarget - 1; i++) {
          byte b1 = BitVectorHelper.getBitsFromCurrentByte(this.validityBuffer,
                  firstByteSource + i, offset);
          byte b2 = BitVectorHelper.getBitsFromNextByte(this.validityBuffer,
                  firstByteSource + i + 1, offset);

          target.validityBuffer.setByte(i, (b1 + b2));
        }

        /* Copying the last piece is done in the following manner:
         * if the source vector has 1 or more bytes remaining, we copy
         * the last piece as a byte formed by shifting data
         * from the current byte and the next byte.
         *
         * if the source vector has no more bytes remaining
         * (we are at the last byte), we copy the last piece as a byte
         * by shifting data from the current byte.
         */
        if ((firstByteSource + byteSizeTarget - 1) < lastByteSource) {
          byte b1 = BitVectorHelper.getBitsFromCurrentByte(this.validityBuffer,
                  firstByteSource + byteSizeTarget - 1, offset);
          byte b2 = BitVectorHelper.getBitsFromNextByte(this.validityBuffer,
                  firstByteSource + byteSizeTarget, offset);

          target.validityBuffer.setByte(byteSizeTarget - 1, b1 + b2);
        } else {
          byte b1 = BitVectorHelper.getBitsFromCurrentByte(this.validityBuffer,
                  firstByteSource + byteSizeTarget - 1, offset);
          target.validityBuffer.setByte(byteSizeTarget - 1, b1);
        }
      }
    }
  }

  final public long getValidityBufferAddress() {
    return (validityBuffer.memoryAddress());
  }

  @Override
  final public ArrowBuf getValidityBuffer() {
    return validityBuffer;
  }

  final public int getValidityBufferValueCapacity() {
    return (int) (validityBuffer.capacity() * 8L);
  }

  @Override
  public String toString() {
    return super.toString() + "[name = " + name + ", ...]";
  }

  @Override
  public void clear() {
  }

  @Override
  public void close() {
    clear();
  }

  @Override
  public TransferPair getTransferPair(BufferAllocator allocator) {
    return getTransferPair(name, allocator);
  }

  @Override
  public Iterator<ValueVector> iterator() {
    return Collections.emptyIterator();
  }

  public static boolean checkBufRefs(final ValueVector vv) {
    for (final ArrowBuf buffer : vv.getBuffers(false)) {
      if (buffer.refCnt() <= 0) {
        throw new IllegalStateException("zero refcount");
      }
    }

    return true;
  }

  @Override
  public BufferAllocator getAllocator() {
    return allocator;
  }

  protected void compareTypes(BaseValueVector target, String caller) {
    if (this.getMinorType() != target.getMinorType()) {
      throw new UnsupportedOperationException(caller + " should have vectors of exact same type");
    }
  }

  protected ArrowBuf releaseBuffer(ArrowBuf buffer) {
    buffer.release();
    buffer = allocator.getEmpty();
    return buffer;
  }

  protected void releaseValidityBuffer() {
    this.validityBuffer.release();
    this.validityBuffer = allocator.getEmpty();
  }

  protected void zeroOutValidity() {
    validityBuffer.setZero(0, validityBuffer.capacity());
  }

  /* number of bytes for the validity buffer for the given valueCount */
  protected static int getValidityBufferSizeFromCount(final int valueCount) {
    return (int) Math.ceil(valueCount / 8.0);
  }


  /**
   * Check if element at given index is null.
   *
   * @param index  position of element
   * @return true if element at given index is null, false otherwise
   */
  @Override
  final public boolean isNull(int index) {
    return (isSet(index) == 0);
  }

  /**
   * Same as {@link #isNull(int)}.
   *
   * @param index  position of element
   * @return 1 if element at given index is not null, 0 otherwise
   */
  final public int isSet(int index) {
    final int byteIndex = index >> 3;
    final byte b = validityBuffer.getByte(byteIndex);
    final int bitIndex = index & 7;
    return (b >> bitIndex) & 0x01;
  }

  /**
   * Get the number of elements that are null in the vector
   *
   * @return the number of null elements.
   */
  @Override
  final public int getNullCount() {
    return BitVectorHelper.getNullCount(validityBuffer, getValueCount());
  }

  final public void markValidityBitToOne(int index){
    BitVectorHelper.setValidityBitToOne(validityBuffer, index);
  }

  final public void markValidityBitToZero(int index){
    BitVectorHelper.setValidityBit(validityBuffer, index, 0);
  }

  final public void setValidityBit(int index, int value){
    BitVectorHelper.setValidityBit(validityBuffer, index, value);
  }

  final void setBitMaskedByte(int byteIndex, byte bitMask){
    BitVectorHelper.setBitMaskedByte(validityBuffer, byteIndex, bitMask);
  }
}

