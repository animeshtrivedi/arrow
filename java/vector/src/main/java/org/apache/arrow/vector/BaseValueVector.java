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

  protected BaseValueVector(String name, BufferAllocator allocator) {
    this.allocator = Preconditions.checkNotNull(allocator, "allocator cannot be null");
    this.name = name;
    this.validityBuffer = this.allocator.getEmpty();
    this.validityAllocationSizeInBytes = getValidityBufferSizeFromCount(INITIAL_VALUE_ALLOCATION);
  }

  void loadValidityBuffer(final ArrowFieldNode fieldNode,
                     final ArrowBuf sourceValidityBuffer,
                     final BufferAllocator allocator){
    this.validityBuffer.release();
    BitVectorHelper.loadValidityBuffer(fieldNode, this.validityBuffer, allocator);
    //set the count here
  }

  public int getValidityCapacity(){
    return this.validityBuffer.capacity();
  }

  public void add(List<ArrowBuf> list){
    list.add(this.validityBuffer);
  }

  public void setReaderIndex(int index){
    this.validityBuffer.readerIndex(index);
  }

  public void setWriterIndex(int index){
    this.validityBuffer.writerIndex(index);
  }

  public void allocateValidityBuffer(final long size) {
    final int curSize = (int) size;
    validityBuffer = allocator.buffer(curSize);
    validityBuffer.readerIndex(0);
    validityAllocationSizeInBytes = curSize;
    validityBuffer.setZero(0, validityBuffer.capacity());
  }

  public void reallocValidityBuffer() {
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

  @Override
  final public long getValidityBufferAddress() {
    return (validityBuffer.memoryAddress());
  }

  @Override
  final public ArrowBuf getValidityBuffer() {
    return validityBuffer;
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

  /* number of bytes for the validity buffer for the given valueCount */
  protected static int getValidityBufferSizeFromCount(final int valueCount) {
    return (int) Math.ceil(valueCount / 8.0);
  }
}

