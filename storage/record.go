package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

func (bn *btreeNode) encodeInternalCell() (*bytes.Buffer, error) {
	writeErr := func(err error) error {
		return fmt.Errorf("binary write failed: %w", err)
	}
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, InternalCell)
	if err != nil {
		return nil, writeErr(err)
	}
	err = binary.Write(buf, binary.LittleEndian, bn.position)
	if err != nil {
		return nil, writeErr(err)
	}
	err = binary.Write(buf, binary.LittleEndian, bn.lastLSN)
	if err != nil {
		return nil, writeErr(err)
	}
	err = binary.Write(buf, binary.LittleEndian, bn.rightOffset)
	if err != nil {
		return nil, writeErr(err)
	}

	cellCount := uint32(len(bn.slots))
	err = binary.Write(buf, binary.LittleEndian, cellCount)
	if err != nil {
		return nil, writeErr(err)
	}
	for i := uint32(0); i < cellCount; i++ {
		err = binary.Write(buf, binary.LittleEndian, bn.slots[i])
		if err != nil {
			return nil, writeErr(err)
		}
	}
	footerBuf := new(bytes.Buffer)

	for i := uint32(0); i < cellCount; i++ {
		cell := bn.internalCells[bn.slots[i]]
		err = binary.Write(footerBuf, binary.LittleEndian, cell.key)
		if err != nil {
			return nil, writeErr(err)
		}
		err = binary.Write(footerBuf, binary.LittleEndian, cell.offset)
		if err != nil {
			return nil, writeErr(err)
		}
	}
	// -2 is the freeSize field itself.
	freeSize := uint16(pageSize - buf.Len() - footerBuf.Len() - 2)
	// encoding the free buffer that separates the header.
	err = binary.Write(buf, binary.LittleEndian, freeSize)
	if err != nil {
		return nil, writeErr(err)
	}
	err = binary.Write(buf, binary.LittleEndian, make([]byte, freeSize))
	if err != nil {
		return nil, writeErr(err)
	}
	_, err = buf.Write(footerBuf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("writing footer into buf failed: %w", err)
	}
	if buf.Len() != pageSize {
		panic(fmt.Sprintf(
			"invalid page size. want=%d. got=%d", pageSize, buf.Len(),
		))
	}
	return buf, nil
}

func (bn *btreeNode) decodeInternalCell(buf *bytes.Buffer) error {
	readErr := func(err error) error {
		return fmt.Errorf("binary read failed: %w", err)
	}
	var cellType byte
	err := binary.Read(buf, binary.LittleEndian, &cellType)
	if err != nil {
		return readErr(err)
	}
	if cellType != InternalCell {
		return fmt.Errorf(
			"invalid node type. want=%d. got=%d", InternalCell, cellType,
		)
	}
	err = binary.Read(buf, binary.LittleEndian, bn.position)
	if err != nil {
		return readErr(err)
	}
	err = binary.Read(buf, binary.LittleEndian, &bn.lastLSN)
	if err != nil {
		return readErr(err)
	}
	err = binary.Read(buf, binary.LittleEndian, &bn.rightOffset)
	if err != nil {
		return readErr(err)
	}
	var cellCount uint32

	err = binary.Read(buf, binary.LittleEndian, &cellCount)
	if err != nil {
		return readErr(err)
	}
	for i := uint32(0); i < cellCount; i++ {
		var offset uint16
		err = binary.Read(buf, binary.LittleEndian, &offset)
		if err != nil {
			return readErr(err)
		}
		bn.slots = append(bn.slots, offset)
	}
	err = binary.Read(buf, binary.LittleEndian, &bn.freeSize)
	if err != nil {
		return readErr(err)
	}
	// skip the empty space
	buf.Next(int(bn.freeSize))

	bn.internalCells = make([]*internalCell, cellCount)
	for i := uint32(0); i < cellCount; i++ {
		var cell internalCell

		err = binary.Read(buf, binary.LittleEndian, &cell.key)
		if err != nil {
			return readErr(err)
		}
		err = binary.Read(buf, binary.LittleEndian, &cell.offset)
		if err != nil {
			return readErr(err)
		}
		bn.internalCells[bn.slots[i]] = &cell
	}
	return nil
}

func (bn *btreeNode) encodeLeafCell() (*bytes.Buffer, error) {
	writeErr := func(err error) error {
		return fmt.Errorf("binary write failed: %w", err)
	}
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, LeafCell)
	if err != nil {
		return nil, writeErr(err)
	}
	err = binary.Write(buf, binary.LittleEndian, bn.position)
	if err != nil {
		return nil, writeErr(err)
	}
	err = binary.Write(buf, binary.LittleEndian, bn.lastLSN)
	if err != nil {
		return nil, writeErr(err)
	}
	err = binary.Write(buf, binary.LittleEndian, bn.hasLSib)
	if err != nil {
		return nil, writeErr(err)
	}
	err = binary.Write(buf, binary.LittleEndian, bn.hasRSib)
	if err != nil {
		return nil, writeErr(err)
	}
	err = binary.Write(buf, binary.LittleEndian, bn.lSibOffset)
	if err != nil {
		return nil, writeErr(err)
	}
	err = binary.Write(buf, binary.LittleEndian, bn.rSibOffset)
	if err != nil {
		return nil, writeErr(err)
	}

	cellCount := uint32(len(bn.slots))
	err = binary.Write(buf, binary.LittleEndian, cellCount)
	if err != nil {
		return nil, writeErr(err)
	}
	for i := uint32(0); i < cellCount; i++ {
		err = binary.Write(buf, binary.LittleEndian, bn.slots[i])
		if err != nil {
			return nil, writeErr(err)
		}
	}
	footerBuf := new(bytes.Buffer)

	for i := uint32(0); i < cellCount; i++ {
		cell := bn.leafCells[bn.slots[i]]
		err = binary.Write(footerBuf, binary.LittleEndian, cell.key)
		if err != nil {
			return nil, writeErr(err)
		}
		err = binary.Write(footerBuf, binary.LittleEndian, cell.deleted)
		if err != nil {
			return nil, writeErr(err)
		}
		err = binary.Write(footerBuf, binary.LittleEndian, cell.valueSize)
		if err != nil {
			return nil, writeErr(err)
		}
		err = binary.Write(footerBuf, binary.LittleEndian, cell.value)
		if err != nil {
			return nil, writeErr(err)
		}
	}
	// -2 is the freeSize field itself.
	freeSize := uint16(pageSize - buf.Len() - footerBuf.Len() - 2)
	// encoding the free buffer that separates the header.
	err = binary.Write(buf, binary.LittleEndian, freeSize)
	if err != nil {
		return nil, writeErr(err)
	}
	err = binary.Write(buf, binary.LittleEndian, make([]byte, freeSize))
	if err != nil {
		return nil, writeErr(err)
	}
	_, err = buf.Write(footerBuf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("writing footer into buf failed: %w", err)
	}
	if buf.Len() != pageSize {
		panic(fmt.Sprintf(
			"invalid page size. want=%d. got=%d", pageSize, buf.Len(),
		))
	}
	return buf, nil
}

func (bn *btreeNode) decodeLeafCell(buf *bytes.Buffer) error {
	readErr := func(err error) error {
		return fmt.Errorf("binary read failed: %w", err)
	}
	var cellType byte
	err := binary.Read(buf, binary.LittleEndian, &cellType)
	if err != nil {
		return readErr(err)
	}
	if cellType != LeafCell {
		return fmt.Errorf("invalid node type. want=%d. got=%d", LeafCell, cellType)
	}
	err = binary.Read(buf, binary.LittleEndian, bn.position)
	if err != nil {
		return readErr(err)
	}
	err = binary.Read(buf, binary.LittleEndian, &bn.lastLSN)
	if err != nil {
		return readErr(err)
	}
	err = binary.Read(buf, binary.LittleEndian, &bn.hasLSib)
	if err != nil {
		return readErr(err)
	}
	err = binary.Read(buf, binary.LittleEndian, &bn.hasRSib)
	if err != nil {
		return readErr(err)
	}
	err = binary.Read(buf, binary.LittleEndian, &bn.lSibOffset)
	if err != nil {
		return readErr(err)
	}
	err = binary.Read(buf, binary.LittleEndian, &bn.rSibOffset)
	if err != nil {
		return readErr(err)
	}
	var cellCount uint32

	err = binary.Read(buf, binary.LittleEndian, &cellCount)
	if err != nil {
		return readErr(err)
	}
	for i := uint32(0); i < cellCount; i++ {
		var offset uint16
		err = binary.Read(buf, binary.LittleEndian, &offset)
		if err != nil {
			return readErr(err)
		}
		bn.slots = append(bn.slots, offset)
	}
	err = binary.Read(buf, binary.LittleEndian, &bn.freeSize)
	if err != nil {
		return readErr(err)
	}
	// skip the empty space
	buf.Next(int(bn.freeSize))

	bn.leafCells = make([]*leafCell, cellCount)
	for i := uint32(0); i < cellCount; i++ {
		var cell leafCell

		err = binary.Read(buf, binary.LittleEndian, &cell.key)
		if err != nil {
			return readErr(err)
		}
		err = binary.Read(buf, binary.LittleEndian, &cell.deleted)
		if err != nil {
			return readErr(err)
		}
		err = binary.Read(buf, binary.LittleEndian, &cell.valueSize)
		if err != nil {
			return readErr(err)
		}
		byteBuf := make([]byte, cell.valueSize)
		if _, err = buf.Read(byteBuf); err != nil {
			return fmt.Errorf("failed to read value: %w", err)
		}
		cell.value = byteBuf
		bn.leafCells[bn.slots[i]] = &cell
	}
	return nil
}
