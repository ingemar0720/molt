package mysqlconv

import (
	"strings"
	"time"

	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroachdb-parser/pkg/util/json"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/molt/parsectx"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/lib/pq/oid"
)

func ConvertRowValue(typMap *pgtype.Map, val []byte, typOID oid.Oid) (tree.Datum, error) {
	if val == nil {
		return tree.DNull, nil
	}
	switch typOID {
	case pgtype.VarcharOID, pgtype.TextOID:
		return tree.NewDString(string(val)), nil
	case pgtype.Float4OID, pgtype.Float8OID:
		return tree.ParseDFloat(string(val))
	case pgtype.Int2OID, pgtype.Int4OID, pgtype.Int8OID:
		return tree.ParseDInt(string(val))
	case pgtype.JSONOID, pgtype.JSONBOID:
		j, err := json.MakeJSON(string(val))
		if err != nil {
			return nil, errors.Wrapf(err, "error decoding json for %v", val)
		}
		return tree.NewDJSON(j), nil
	case pgtype.TimestampOID:
		v := string(val)
		if strings.HasPrefix(v, "0000-") {
			return tree.DNull, nil
		}
		ret, _, err := tree.ParseDTimestamp(parsectx.ParseContext, v, time.Microsecond)
		return ret, err
	case pgtype.TimestamptzOID:
		v := string(val)
		ret, _, err := tree.ParseDTimestampTZ(parsectx.ParseContext, v, time.Microsecond)
		return ret, err
	case pgtype.DateOID:
		ret, _, err := tree.ParseDDate(parsectx.ParseContext, string(val))
		return ret, err
	case pgtype.ByteaOID:
		return tree.NewDBytes(tree.DBytes(val)), nil
	case pgtype.NumericOID:
		return tree.ParseDDecimal(string(val))
	case pgtype.BitOID, pgtype.VarbitOID:
		return tree.ParseDBitArray(string(val))
	case oid.T_anyenum:
		return tree.NewDString(string(val)), nil
	}
	return nil, errors.AssertionFailedf("value type OID %d not yet translatable", typOID)
}

func ConvertRowValues(typMap *pgtype.Map, vals [][]byte, typOIDs []oid.Oid) (tree.Datums, error) {
	ret := make(tree.Datums, len(vals))
	if len(vals) != len(typOIDs) {
		return nil, errors.AssertionFailedf("val length != oid length: %v vs %v", vals, typOIDs)
	}
	for i := range vals {
		var err error
		if ret[i], err = ConvertRowValue(typMap, vals[i], typOIDs[i]); err != nil {
			return nil, err
		}
	}
	return ret, nil
}
