package subsetter

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/samber/lo"
)

const RuleAll = "1=1"

type Rule struct {
	Table string
	Where string
}

func (r *Rule) String() string {
	return fmt.Sprintf("%s:%s", r.Table, r.Where)
}

func (r *Rule) Query(exclude []string) string {
	if r.Where == "" {
		return fmt.Sprintf("SELECT * FROM %s", r.Table)
	}

	if len(exclude) > 0 {
		exclude = lo.Map(exclude, func(s string, _ int) string {
			return QuoteString(s)
		})
		r.Where = fmt.Sprintf("%s AND id NOT IN (%s)", r.Where, strings.Join(exclude, ","))
	}
	return fmt.Sprintf("SELECT * FROM %s WHERE %s", r.Table, r.Where)
}

func GetPrimaryKeyNameRel(t Table, relatedTable string) string {
	log.Debug().Str("table", t.Name).Str("relatedTable", relatedTable).Msg("Getting primary key name for related table")
	for _, r := range t.RequiredBy {
		if r.ForeignTable == relatedTable {
			return r.PrimaryColumn
		}
	}
	for _, r := range t.Relations {
		if r.ForeignTable == relatedTable {
			return r.PrimaryColumn
		}
	}
	panic(fmt.Sprintf("No primary key found for table %s", t.Name))
}

func (r *Rule) QueryInclude(include []string, relatedTable Table) string {
	q := fmt.Sprintf("SELECT * FROM %s", relatedTable.Name)
	relatedTableKey := GetPrimaryKeyNameRel(relatedTable, r.Table)

	if len(include) > 0 {
		include = lo.Map(include, func(s string, _ int) string {
			return QuoteString(s)
		})
		q = fmt.Sprintf("%s WHERE %s IN (%s)", q, relatedTableKey, strings.Join(include, ","))
	}
	log.Debug().Str("query", q).Msgf("Query for related table %s", relatedTable.Name)
	return q
}

func (r *Rule) Copy(s *Sync) (err error) {
	log.Debug().Str("query", r.Where).Msgf("Transferring forced rows for table %s", r.Table)
	var data string

	keyName, err := GetPrimaryKeyName(r.Table, s.destination)
	if err != nil {
		return errors.Wrapf(err, "Error getting primary key for table %s", r.Table)
	}

	q := fmt.Sprintf(`SELECT %s FROM %s`, keyName, r.Table)
	log.Debug().Str("query", q).Msgf("Getting keys for %s from target", r.Table)

	excludedIDs := []string{}
	if primaryKeys, err := GetKeys(q, s.destination); err == nil {
		excludedIDs = primaryKeys
	}
	log.Debug().Strs("excludedIDs", excludedIDs).Msgf("Excluded IDs for table %s", r.Table)

	if data, err = CopyQueryToString(r.Query(excludedIDs), s.source); err != nil {
		return errors.Wrapf(err, "Error copying forced rows for table %s", r.Table)
	}
	if err = CopyStringToTable(r.Table, data, s.destination); err != nil {
		return errors.Wrapf(err, "Error inserting forced rows for table %s", r.Table)
	}
	log.Debug().Str("table", r.Table).Msgf("Transfered rows")
	return
}

func (r *Rule) CopyRelated(s *Sync, relatedTable Table) (err error) {
	log.Debug().Str("query", r.Where).Msgf("Transferring forced rows for table %s", r.Table)
	var data string

	keyName, err := GetPrimaryKeyName(r.Table, s.destination)
	if err != nil {
		return errors.Wrapf(err, "Error getting primary key for table %s", r.Table)
	}

	q := fmt.Sprintf(`SELECT %s FROM %s WHERE %s`, keyName, r.Table, r.Where)
	log.Debug().Str("query", q).Msgf("Getting keys for %s from target", r.Table)

	includedIDs := []string{}
	if primaryKeys, err := GetKeys(q, s.source); err == nil {
		includedIDs = primaryKeys
	}
	log.Debug().Strs("includedIDs", includedIDs).Str("table", relatedTable.Name).Msgf("Included IDs for table %s", r.Table)

	if data, err = CopyQueryToString(r.QueryInclude(includedIDs, relatedTable), s.source); err != nil {
		return errors.Wrapf(err, "Error copying forced rows for table %s", r.Table)
	}
	if err = CopyStringToTable(r.Table, data, s.destination); err != nil {
		return errors.Wrapf(err, "Error inserting forced rows for table %s", r.Table)
	}
	log.Debug().Str("table", relatedTable.Name).Msgf("Transfered related rows")
	return
}
