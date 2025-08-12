package sqlx

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/shopspring/decimal"
	"github.com/xwb1989/sqlparser"
)

func ExecuteAlter(ctx context.Context, db *sqlx.DB, sqlStr string) error {
	_, err := db.ExecContext(ctx, sqlStr)
	return err
}

func ExecuteQuery(ctx context.Context, db *sqlx.DB, sqlStr string) ([]map[string]any, error) {
	stmt, err := sqlparser.Parse(sqlStr)
	if err != nil {
		return nil, fmt.Errorf("SQL parse error: %v", err)
	}

	// 只允许 SELECT
	if _, ok := stmt.(*sqlparser.Select); !ok {
		return nil, fmt.Errorf("only SELECT statements are allowed")
	}

	rows, err := db.QueryxContext(ctx, sqlStr)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []map[string]any
	for rows.Next() {
		rowMap := make(map[string]any)
		if err := rows.MapScan(rowMap); err != nil {
			return nil, err
		}

		// 转换为 JSON 兼容格式（比如 []byte → string，int64 → json.Number）
		jsonCompatible := make(map[string]interface{})
		for k, v := range rowMap {
			switch val := v.(type) {
			case []byte:
				jsonCompatible[k] = string(val)
			case int64, float64, bool, string, int, int32, nil:
				jsonCompatible[k] = val
			case sql.NullString:
				if val.Valid {
					jsonCompatible[k] = val.String
				} else {
					jsonCompatible[k] = nil
				}
			case sql.NullInt64:
				if val.Valid {
					jsonCompatible[k] = val.Int64
				} else {
					jsonCompatible[k] = nil
				}
			case decimal.Decimal:
				jsonCompatible[k] = val.String()
			default:
				// fallback 转字符串
				jsonCompatible[k] = fmt.Sprintf("%v", val)
			}
		}

		results = append(results, jsonCompatible)
	}

	return results, nil
}
