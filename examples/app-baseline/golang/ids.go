package main

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
)

type UUID [16]byte

const (
	nsOrg        byte = 0x10
	nsUser       byte = 0x11
	nsProject    byte = 0x12
	nsTicket     byte = 0x13
	nsComment    byte = 0x14
	nsLabel      byte = 0x15
	nsWriteProbe byte = 0x7f
	nsLoadWrite  byte = 0x7e

	statusOpen       = "open"
	statusClosed     = "closed"
	statusInProgress = "in_progress"
)

func uuidFromOrdinal(namespace byte, ordinal uint64) UUID {
	var data UUID
	for i := range data {
		data[i] = namespace
	}
	data[6] = 0x70 | (namespace & 0x0f)
	binary.BigEndian.PutUint64(data[8:], ordinal)
	data[8] = 0x80 | (data[8] & 0x3f)
	return data
}

func formatUUID(value UUID) string {
	hexed := hex.EncodeToString(value[:])
	return fmt.Sprintf("%s-%s-%s-%s-%s", hexed[0:8], hexed[8:12], hexed[12:16], hexed[16:20], hexed[20:32])
}

func encodeShort(value UUID) string {
	return hex.EncodeToString(value[12:16])
}

func sqlStatusToRiff(status string) string {
	switch status {
	case statusOpen:
		return "Open"
	case statusClosed:
		return "Closed"
	case statusInProgress:
		return "InProgress"
	default:
		panic(fmt.Sprintf("unknown ticket status %s", status))
	}
}

func uuidEquals(left, right UUID) bool {
	return left == right
}
