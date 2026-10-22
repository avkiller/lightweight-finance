package qianji

import (
	"github.com/mayswind/ezbookkeeping/pkg/converters/datatable"
	"github.com/mayswind/ezbookkeeping/pkg/core"
	"github.com/mayswind/ezbookkeeping/pkg/errs"
	"github.com/mayswind/ezbookkeeping/pkg/log"
)

func createNewQianjiAppTransactionBasicDataTable(ctx core.Context, originalDataTable datatable.BasicDataTable) (datatable.BasicDataTable, error) {
	toIndex := originalDataTable.DataRowCount()

	// 钱迹导出文件第一行即为列头，无需跳过任何前缀行，直接从第 0 行开始
	if toIndex < 2 {
		log.Errorf(ctx, "[qianji_app_transaction_data_extrator.createNewQianjiAppTransactionBasicDataTable] cannot parse import data, because data table row count is less 1")
		return nil, errs.ErrNotFoundTransactionDataInFile
	}

	return datatable.CreateSubBasicTable(originalDataTable, 0, toIndex, true), nil
}
