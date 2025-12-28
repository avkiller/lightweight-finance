package qianji

import (
	"bytes"
	"time"

	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"

	"github.com/mayswind/ezbookkeeping/pkg/converters/converter"
	"github.com/mayswind/ezbookkeeping/pkg/converters/csv"
	"github.com/mayswind/ezbookkeeping/pkg/converters/datatable"
	"github.com/mayswind/ezbookkeeping/pkg/core"
	"github.com/mayswind/ezbookkeeping/pkg/models"
)

const qianjiAppTransactionTimeColumnName = "时间"
const qianjiAppTransactionTypeColumnName = "类型"
const qianjiAppTransactionCategoryColumnName = "分类"
const qianjiAppTransactionSubCategoryColumnName = "二级分类"
const qianjiAppTransactionAccountNameColumnName = "记账者"
const qianjiAppTransactionAccountCurrencyColumnName = "币种"
const qianjiAppTransactionAmountColumnName = "金额"
const qianjiAppTransactionDescriptionColumnName = "备注"
const qianjiAppTransactionRelatedIdColumnName = "关联账单"
const qianjiAppTransactionTagsColumnName = "标签"

var qianjiAppDataColumnNameMapping = map[datatable.TransactionDataTableColumn]string{
	datatable.TRANSACTION_DATA_TABLE_TRANSACTION_TIME:     qianjiAppTransactionTimeColumnName,
	datatable.TRANSACTION_DATA_TABLE_TRANSACTION_TYPE:     qianjiAppTransactionTypeColumnName,
	datatable.TRANSACTION_DATA_TABLE_CATEGORY:             qianjiAppTransactionCategoryColumnName,
	datatable.TRANSACTION_DATA_TABLE_SUB_CATEGORY:         qianjiAppTransactionSubCategoryColumnName,
	datatable.TRANSACTION_DATA_TABLE_ACCOUNT_NAME:         qianjiAppTransactionAccountNameColumnName,
	datatable.TRANSACTION_DATA_TABLE_ACCOUNT_CURRENCY:     qianjiAppTransactionAccountCurrencyColumnName,
	datatable.TRANSACTION_DATA_TABLE_AMOUNT:               qianjiAppTransactionAmountColumnName,
	datatable.TRANSACTION_DATA_TABLE_RELATED_ACCOUNT_NAME: qianjiAppTransactionRelatedIdColumnName,
	datatable.TRANSACTION_DATA_TABLE_TAGS:                 qianjiAppTransactionTagsColumnName,
	datatable.TRANSACTION_DATA_TABLE_DESCRIPTION:          qianjiAppTransactionDescriptionColumnName,
}

// qianjiAppTransactionDataCsvFileImporter defines the structure of feidee mymoney app csv importer for transaction data
type qianjiAppTransactionDataCsvFileImporter struct{}

// Initialize a feidee mymoney app transaction data csv file importer singleton instance
var (
	QianjiTransactionDataCsvFileImporter = &qianjiAppTransactionDataCsvFileImporter{}
)

// ParseImportedData returns the imported data by parsing the feidee mymoney app transaction csv data
func (c *qianjiAppTransactionDataCsvFileImporter) ParseImportedData(ctx core.Context, user *models.User, data []byte, defaultTimezone *time.Location, additionalOptions converter.TransactionDataImporterOptions, accountMap map[string]*models.Account, expenseCategoryMap map[string]map[string]*models.TransactionCategory, incomeCategoryMap map[string]map[string]*models.TransactionCategory, transferCategoryMap map[string]map[string]*models.TransactionCategory, tagMap map[string]*models.TransactionTag) (models.ImportedTransactionSlice, []*models.Account, []*models.TransactionCategory, []*models.TransactionCategory, []*models.TransactionCategory, []*models.TransactionTag, error) {
	fallback := unicode.UTF8.NewDecoder()
	reader := transform.NewReader(bytes.NewReader(data), unicode.BOMOverride(fallback))

	dataTable, err := csv.CreateNewCsvBasicDataTable(ctx, reader, true)

	if err != nil {
		return nil, nil, nil, nil, nil, nil, err
	}

	transactionRowParser := createQianjiTransactionDataRowParser()
	transactionDataTable := datatable.CreateNewTransactionDataTableFromBasicDataTableWithRowParser(dataTable, qianjiAppDataColumnNameMapping, transactionRowParser)
	dataTableImporter := converter.CreateNewSimpleImporterWithTypeNameMapping(qianjiTransactionTypeNameMapping)

	return dataTableImporter.ParseImportedData(ctx, user, transactionDataTable, defaultTimezone, additionalOptions, accountMap, expenseCategoryMap, incomeCategoryMap, transferCategoryMap, tagMap)
}
