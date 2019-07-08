// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"sync/atomic"

	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/memdb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

func (s *session) pickMemdbLevel(umin, umax []byte, maxLevel int) int {
	v := s.version()
	defer v.release()
	return v.pickMemdbLevel(umin, umax, maxLevel)
}

func (s *session) flushMemdb(rec *sessionRecord, mdb *memdb.DB, maxLevel int) (int, error) {
	// Create sorted table.
	iter := mdb.NewIterator(nil)
	defer iter.Release()
	//数据从iter读出来，写入表文件中
	t, n, err := s.tops.createFrom(iter)
	if err != nil {
		return 0, err
	}

	// Pick level other than zero can cause compaction issue with large
	// bulk insert and delete on strictly incrementing key-space. The
	// problem is that the small deletion markers trapped at lower level,
	// while key/value entries keep growing at higher level. Since the
	// key-space is strictly incrementing it will not overlaps with
	// higher level, thus maximum possible level is always picked, while
	// overlapping deletion marker pushed into lower level.
	//除零之外的选择级别可能导致大批量插入的压缩问题和严格递增密钥空间时的删除。
	// 问题是小的删除标记被困在较低级别，而键/值条目继续在较高级别上增长。
	// 由于密钥空间严格递增，它不会与更高级别重叠，因此总是选择最大可能级别，而重叠删除标记被推入较低级别。
	// See: https://github.com/syndtr/goleveldb/issues/127.
	//返回当前文件所在的level，在memcompaction操作中，level为0
	//解决：出现删除不掉的问题
	flushLevel := s.pickMemdbLevel(t.imin.ukey(), t.imax.ukey(), maxLevel)
	//fmt.Println("++++++++flushlevel",flushLevel, maxLevel)
	rec.addTableFile(flushLevel, t)

	s.logf("memdb@flush created L%d@%d N·%d S·%s %q:%q", flushLevel, t.fd.Num, n, shortenb(int(t.size)), t.imin, t.imax)
	return flushLevel, nil
}

//存在score>=1或者
// Pick a compaction based on current state; need external synchronization.
func (s *session) pickCompaction() *compaction {
	v := s.version()

	var sourceLevel int
	var t0 tFiles
	if v.cScore >= 1 {
		sourceLevel = v.cLevel
		cptr := s.getCompPtr(sourceLevel) //当前会话对应的级别sourceLevel的最大key
		tables := v.versionLevels[sourceLevel]
		for _, t := range tables { //如果表的最大值 大于 会话级别对应的最大key,那么表可能需要重新构建（需要compaction）
			if cptr == nil || s.icmp.Compare(t.imax, cptr) > 0 {
				t0 = append(t0, t)
				break
			}
		}
		if len(t0) == 0 { //仅仅把表[0]压缩到下一个Level??????
			t0 = append(t0, tables[0])
		}
	} else {
		if p := atomic.LoadPointer(&v.cSeek); p != nil { //kv被多次读
			ts := (*tSet)(p)
			sourceLevel = ts.level
			t0 = append(t0, ts.table)
		} else {
			v.release()
			return nil
		}
	}

	return newCompaction(s, v, sourceLevel, t0)
}

// Create compaction from given level and range; need external synchronization.
func (s *session) getCompactionRange(sourceLevel int, umin, umax []byte, noLimit bool) *compaction {
	v := s.version()

	if sourceLevel >= len(v.versionLevels) {
		v.release()
		return nil
	}

	t0 := v.versionLevels[sourceLevel].getOverlaps(nil, s.icmp, umin, umax, sourceLevel == 0)
	if len(t0) == 0 {
		v.release()
		return nil
	}

	// Avoid compacting too much in one shot in case the range is large.
	// But we cannot do this for level-0 since level-0 files can overlap
	// and we must not pick one file and drop another older file if the
	// two files overlap.
	// 压缩的数据量不要太多
	if !noLimit && sourceLevel > 0 {
		limit := int64(v.s.o.GetCompactionSourceLimit(sourceLevel))
		total := int64(0)
		for i, t := range t0 {
			total += t.size
			if total >= limit {
				s.logf("table@compaction limiting F·%d -> F·%d", len(t0), i+1)
				t0 = t0[:i+1]
				break
			}
		}
	}

	return newCompaction(s, v, sourceLevel, t0)
}

func newCompaction(s *session, v *version, sourceLevel int, t0 tFiles) *compaction {
	c := &compaction{
		s:             s,
		v:             v,
		sourceLevel:   sourceLevel,
		levels:        [2]tFiles{t0, nil},
		maxGPOverlaps: int64(s.o.GetCompactionGPOverlaps(sourceLevel)),
		tPtrs:         make([]int, len(v.versionLevels)),
	}
	c.expand()
	c.save()
	return c
}

// compaction represent a compaction state.
type compaction struct {
	s *session
	v *version

	sourceLevel   int
	levels        [2]tFiles //把sourceLevel压缩到sourceLevel+1;此时sourceLevel和sourceLevel+1级对应的key可能有重叠，因此会把重叠的sourceLevel+1部分放入到levels[1]中
	maxGPOverlaps int64

	gp                tFiles //// (parent == sourceLevel+1; grandparent == sourceLevel+2)
	gpi               int
	seenKey           bool
	gpOverlappedBytes int64
	imin, imax        internalKey
	tPtrs             []int
	released          bool

	snapGPI               int
	snapSeenKey           bool
	snapGPOverlappedBytes int64
	snapTPtrs             []int
}

func (c *compaction) save() {
	c.snapGPI = c.gpi
	c.snapSeenKey = c.seenKey
	c.snapGPOverlappedBytes = c.gpOverlappedBytes
	c.snapTPtrs = append(c.snapTPtrs[:0], c.tPtrs...)
}

func (c *compaction) restore() {
	c.gpi = c.snapGPI
	c.seenKey = c.snapSeenKey
	c.gpOverlappedBytes = c.snapGPOverlappedBytes
	c.tPtrs = append(c.tPtrs[:0], c.snapTPtrs...)
}

func (c *compaction) release() {
	if !c.released {
		c.released = true
		c.v.release()
	}
}

// Expand compacted tables; need external synchronization.
// 基于sourcelevel的待compaction文件进行扩展，查找sourceLevel+1级的需要参与compaction的文件
func (c *compaction) expand() {
	// 参与compaction的文件数的上限
	limit := int64(c.s.o.GetCompactionExpandLimit(c.sourceLevel))
	vt0 := c.v.versionLevels[c.sourceLevel]
	vt1 := tFiles{}
	if level := c.sourceLevel + 1; level < len(c.v.versionLevels) {
		vt1 = c.v.versionLevels[level]
	}

	// c.levels是source，source+1层参与compaction的文件
	t0, t1 := c.levels[0], c.levels[1]
	// sourcelevel级的源文件的[imin, imax]
	imin, imax := t0.getRange(c.s.icmp)

	// For non-zero levels, the ukey can't hop across tables at all.
	if c.sourceLevel == 0 {
		// We expand t0 here just incase ukey hop across tables.
		t0 = vt0.getOverlaps(t0, c.s.icmp, imin.ukey(), imax.ukey(), c.sourceLevel == 0)
		if len(t0) != len(c.levels[0]) {
			imin, imax = t0.getRange(c.s.icmp)
		}
	}
	//获取sourceLevel与sourcelevel+1的重叠部分tableFiles
	t1 = vt1.getOverlaps(t1, c.s.icmp, imin.ukey(), imax.ukey(), false)
	// Get entire range covered by compaction.
	amin, amax := append(t0, t1...).getRange(c.s.icmp)

	// See if we can grow the number of inputs in "sourceLevel" without
	// changing the number of "sourceLevel+1" files we pick up.
	//注释来源于https://github.com/shenlongxing/goleveldb-annotated/blob/master/leveldb/session_compaction.go
	// 在不修改level+1的前提下，尽量扩大level层参与compaction的文件数，这么处理的原因是：
	/*
                        a        b         c       d
      source level   <-----><--------><--------><------>
                       A    B     C    D     E     F
      level +1       <---><---><----><----><----><---->
      上面的例子中，比如发起compaction的源文件是b，查找compaction文件的流程如下：
      1. 在source层进行expand，由于不是level 0，不存在overlap。结果还是b
      2. 使用b文件的key范围[min,max]到level+1层进行比较，得到overlap的文件B、C、D
      3. 此时b、B、C、D文件为compaction的输入文件
      但是还有另外一种情况：
                        a      b         c       d
      source level   <---><--------><--------><------>
                         A      B     C    D     E
      level +1       <-------><----><----><----><---->
      这种情况下的compaction流程为：
      1. 在source层进行expand，由于不是level 0，不存在overlap。结果还是b
      2. 使用b文件的key范围[min,max]到level+1层进行比较，得到overlap的文件A、B
      3. 这时候compaction的输入文件是否就是b、A、B了呢？观察一下可以发现，source level的a文件
         key的范围完全在A+B范围内。这时候把a进入到compaction的目标文件，完全不会影响level+1的key的范围
      4. 因此，原则上：在不修改level+1的前提下，尽量扩大level层参与compaction的文件数
      在第一个例子中，能不能把与B、C、D有overlap的a加进来呢？答案是否定的。因为leveldb中，除了level 0
      以外，其他level不能存在key的overlap。如果把a加入到compaction的源文件，生成的结果文件会与A存在overlap。
	*/
	if len(t1) > 0 {
		//使用amin, amax，可以再扩展一些sourcelevel级的文件 并且要compact的文件大小比较小的情况下，继续扩展
		exp0 := vt0.getOverlaps(nil, c.s.icmp, amin.ukey(), amax.ukey(), c.sourceLevel == 0)
		if len(exp0) > len(t0) && t1.size()+exp0.size() < limit {
			xmin, xmax := exp0.getRange(c.s.icmp)
			//使用更大范围的xmin, xmax，可以再vt1中，可能扩展到更多的文件exp1
			exp1 := vt1.getOverlaps(nil, c.s.icmp, xmin.ukey(), xmax.ukey(), false)
			if len(exp1) == len(t1) { //level—1级别的范围不能改变
				c.s.logf("table@compaction expanding L%d+L%d (F·%d S·%s)+(F·%d S·%s) -> (F·%d S·%s)+(F·%d S·%s)",
					c.sourceLevel, c.sourceLevel+1, len(t0), shortenb(int(t0.size())), len(t1), shortenb(int(t1.size())),
					len(exp0), shortenb(int(exp0.size())), len(exp1), shortenb(int(exp1.size())))
				imin, imax = xmin, xmax
				t0, t1 = exp0, exp1
				amin, amax = append(t0, t1...).getRange(c.s.icmp)
			}
		}
	}

	// Compute the set of grandparent files that overlap this compaction
	// (parent == sourceLevel+1; grandparent == sourceLevel+2)
	if level := c.sourceLevel + 2; level < len(c.v.versionLevels) {
		c.gp = c.v.versionLevels[level].getOverlaps(c.gp, c.s.icmp, amin.ukey(), amax.ukey(), false)
	}

	c.levels[0], c.levels[1] = t0, t1
	c.imin, c.imax = imin, imax
}

// Check whether compaction is trivial.检查是否有压缩的必要性
func (c *compaction) trivial() bool {
	return len(c.levels[0]) == 1 && len(c.levels[1]) == 0 && c.gp.size() <= c.maxGPOverlaps
}

//ukey是否和高层的key范围重叠，重叠的话，不属于baselevel(key的范围仅仅在要压缩的2层中).
//如果key不在更高的层中，返回true
func (c *compaction) baseLevelForKey(ukey []byte) bool {
	for level := c.sourceLevel + 2; level < len(c.v.versionLevels); level++ {
		tables := c.v.versionLevels[level]
		for c.tPtrs[level] < len(tables) {
			t := tables[c.tPtrs[level]]
			if c.s.icmp.uCompare(ukey, t.imax.ukey()) <= 0 {
				// We've advanced far enough.
				if c.s.icmp.uCompare(ukey, t.imin.ukey()) >= 0 {
					// Key falls in this file's range, so definitely not base level.
					return false
				}
				break
			}
			c.tPtrs[level]++
		}
	}
	return true
}

func (c *compaction) shouldStopBefore(ikey internalKey) bool {
	for ; c.gpi < len(c.gp); c.gpi++ {
		gp := c.gp[c.gpi]
		if c.s.icmp.Compare(ikey, gp.imax) <= 0 {
			break
		}
		if c.seenKey {
			c.gpOverlappedBytes += gp.size
		}
	}
	c.seenKey = true
	//当前的Key,对应的文件大小已经大于了maxGPOverlaps（当前默认20MB）;会产生一个新的文件（意图猜测：后续的一次压缩触及的文件数量不能太多）
	if c.gpOverlappedBytes > c.maxGPOverlaps {
		// Too much overlap for current output; start new output.
		//fmt.Println("shouldStopBefore:", c.maxGPOverlaps)
		c.gpOverlappedBytes = 0
		return true
	}
	return false
}

// Creates an iterator.
//有重叠的部分如何实现排序的/去重的？？？？？？？？？
func (c *compaction) newIterator() iterator.Iterator {
	// Creates iterator slice.
	icap := len(c.levels)
	if c.sourceLevel == 0 {
		// Special case for level-0.
		icap = len(c.levels[0]) + 1
	}
	its := make([]iterator.Iterator, 0, icap)

	// Options.
	ro := &opt.ReadOptions{
		DontFillCache: true,
		Strict:        opt.StrictOverride,
	}
	strict := c.s.o.GetStrict(opt.StrictCompaction)
	if strict {
		ro.Strict |= opt.StrictReader
	}

	for i, tables := range c.levels {
		if len(tables) == 0 {
			continue
		}

		// Level-0 is not sorted and may overlaps each other.
		if c.sourceLevel+i == 0 {
			for _, t := range tables {
				its = append(its, c.s.tops.newIterator(t, nil, ro))
			}
		} else {
			it := iterator.NewIndexedIterator(tables.newIndexIterator(c.s.tops, c.s.icmp, nil, ro), strict)
			its = append(its, it)
		}
	}

	return iterator.NewMergedIterator(its, c.s.icmp, strict)
}
