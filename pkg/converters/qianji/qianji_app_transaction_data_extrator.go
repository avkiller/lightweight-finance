package qianji

import (
	"strings"

	"github.com/mayswind/ezbookkeeping/pkg/converters/csv"
	"github.com/mayswind/ezbookkeeping/pkg/converters/datatable"
	"github.com/mayswind/ezbookkeeping/pkg/core"
	"github.com/mayswind/ezbookkeeping/pkg/errs"
	"github.com/mayswind/ezbookkeeping/pkg/log"
)

func createNewQianjiAppTransactionBasicDataTable(ctx core.Context, originalDataTable datatable.BasicDataTable) (datatable.BasicDataTable, error) {
	iterator := originalDataTable.DataRowIterator()
	allOriginalLines := make([][]string, 0)

	for iterator.HasNext() {
		row := iterator.Next()

		items := make([]string, row.ColumnCount())

		for i := 0; i < row.ColumnCount(); i++ {
			items[i] = strings.Trim(row.GetData(i), " ")
		}

		allOriginalLines = append(allOriginalLines, items)
	}

	if len(allOriginalLines) < 2 {
		log.Errorf(ctx, "[qianji_app_transaction_data_extrator.createNewQianjiAppTransactionBasicDataTable] cannot parse import data, because data table row count is less 1")
		return nil, errs.ErrNotFoundTransactionDataInFile
	}

	return csv.CreateNewCustomCsvBasicDataTable(allOriginalLines, true), nil
}
