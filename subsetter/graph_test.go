package subsetter

import (
	"testing"

	"github.com/samber/lo"
)

func TestTableGraph(t *testing.T) {

	relations := []Relation{
		{"blog_networks", "id", "blogs", "blog_id"},
		{"users", "id", "blog_networks", "user_id"},
		{"users", "id", "blogs", "user_id"},
		{"users", "id", "users", "owner_id"}, // self reference
		{"users", "id", "collaborator_api_keys", "user_id"},
		{"blogs", "id", "backups", "blog_id"},
		{"blogs", "id", "blog_imports", "blog_id"},
		{"blogs", "id", "blog_imports", "blog_id"},
		{"blogs", "id", "cleanup_notification", "blog_id"},
	}

	got, _ := TableGraph("users", relations)

	if want, _ := lo.Last(got); want != "users" {
		t.Fatalf("TableGraph() = %v, want %v", got, "users")
	}

}
func TestTableGraphNnoRelation(t *testing.T) {

	relations := []Relation{
		{"blog_networks", "id", "blogs", "blog_id"},
		{"users", "id", "blog_networks", "user_id"},
		{"users", "id", "blogs", "user_id"},
		{"users", "id", "users", "owner_id"}, // self reference
		{"users", "id", "collaborator_api_keys", "user_id"},
		{"blogs", "id", "backups", "blog_id"},
		{"blogs", "id", "blog_imports", "blog_id"},
		{"blogs", "id", "blog_imports", "blog_id"},
		{"blogs", "id", "cleanup_notification", "blog_id"},
	}

	got, _ := TableGraph("simple", relations)

	if want, _ := lo.Last(got); want != "simple" {
		t.Fatalf("TableGraph() = %v, want %v", got, "simple")
	}

}
