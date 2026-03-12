package broker

import (
	"testing"
)

func TestGroupManager(t *testing.T) {
	gm := NewGroupManager()

	// 1. Fetch non-existent offset
	offset, found := gm.FetchOffset("group1", "topic1", 0)
	if found {
		t.Fatalf("expected not found, got %d", offset)
	}

	// 2. Commit offset
	gm.CommitOffset("group1", "topic1", 0, 42)

	// 3. Fetch existing offset
	offset, found = gm.FetchOffset("group1", "topic1", 0)
	if !found {
		t.Fatal("expected to find offset")
	}
	if offset != 42 {
		t.Fatalf("expected offset 42, got %d", offset)
	}

	// 4. Update offset
	gm.CommitOffset("group1", "topic1", 0, 100)
	offset, _ = gm.FetchOffset("group1", "topic1", 0)
	if offset != 100 {
		t.Fatalf("expected updated offset 100, got %d", offset)
	}

	// 5. Different group, same topic/partition
	offset, found = gm.FetchOffset("group2", "topic1", 0)
	if found {
		t.Fatalf("expected no offset for group2, got %d", offset)
	}

	// 6. Same group, different partition
	offset, found = gm.FetchOffset("group1", "topic1", 1)
	if found {
		t.Fatalf("expected no offset for partition 1, got %d", offset)
	}
}
