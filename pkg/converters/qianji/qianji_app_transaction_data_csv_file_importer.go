package qianji

import (
	"bytes"
	"strings"

	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"

	"github.com/mayswind/ezbookkeeping/pkg/converters/converter"
	"github.com/mayswind/ezbookkeeping/pkg/converters/csv"
	"github.com/mayswind/ezbookkeeping/pkg/converters/datatable"
	"github.com/mayswind/ezbookkeeping/pkg/core"
	"github.com/mayswind/ezbookkeeping/pkg/errs"
	"github.com/mayswind/ezbookkeeping/pkg/log"
	"github.com/mayswind/ezbookkeeping/pkg/models"
)

const qianjiAppTransactionTimeColumnName = "时间"
const qianjiAppTransactionTypeColumnName = "类型"
const qianjiAppTransactionCategoryColumnName = "分类"
const qianjiAppTransactionSubCategoryColumnName = "二级分类"
const qianjiAppTransactionAccountNameColumnName = "账户"
const qianjiAppTransactionAccountCurrencyColumnName = "币种"
const qianjiAppTransactionAmountColumnName = "金额"
const qianjiAppTransactionDescriptionColumnName = "备注"
const qianjiAppTransactionMemberColumnName = "记账者"
const qianjiAppTransactionTypeIncomeText = "收入"
const qianjiAppTransactionTypeExpenseText = "支出"

var qianjiAppDataColumnNameMapping = map[datatable.TransactionDataTableColumn]string{
	datatable.TRANSACTION_DATA_TABLE_TRANSACTION_TIME: qianjiAppTransactionTimeColumnName,
	datatable.TRANSACTION_DATA_TABLE_TRANSACTION_TYPE: qianjiAppTransactionTypeColumnName,
	datatable.TRANSACTION_DATA_TABLE_CATEGORY:         qianjiAppTransactionCategoryColumnName,
	datatable.TRANSACTION_DATA_TABLE_SUB_CATEGORY:     qianjiAppTransactionSubCategoryColumnName,
	datatable.TRANSACTION_DATA_TABLE_ACCOUNT_NAME:     qianjiAppTransactionAccountNameColumnName,
	datatable.TRANSACTION_DATA_TABLE_ACCOUNT_CURRENCY: qianjiAppTransactionAccountCurrencyColumnName,
	datatable.TRANSACTION_DATA_TABLE_AMOUNT:           qianjiAppTransactionAmountColumnName,
	datatable.TRANSACTION_DATA_TABLE_DESCRIPTION:      qianjiAppTransactionDescriptionColumnName,
	datatable.TRANSACTION_DATA_TABLE_MEMBER:           qianjiAppTransactionMemberColumnName,
}

// qianjiAppTransactionDataCsvFileImporter defines the structure of feidee mymoney app csv importer for transaction data
type qianjiAppTransactionDataCsvFileImporter struct{}

// Initialize a feidee mymoney app transaction data csv file importer singleton instance
var (
	QianjiAppTransactionDataCsvFileImporter = &qianjiAppTransactionDataCsvFileImporter{}
)

// ParseImportedData returns the imported data by parsing the feidee mymoney app transaction csv data
func (c *qianjiAppTransactionDataCsvFileImporter) ParseImportedData(ctx core.Context, user *models.User, data []byte, defaultTimezoneOffset int16, additionalOptions converter.TransactionDataImporterOptions, accountMap map[string]*models.Account, expenseCategoryMap map[string]map[string]*models.TransactionCategory, incomeCategoryMap map[string]map[string]*models.TransactionCategory, transferCategoryMap map[string]map[string]*models.TransactionCategory, tagMap map[string]*models.TransactionTag) (models.ImportedTransactionSlice, []*models.Account, []*models.TransactionCategory, []*models.TransactionCategory, []*models.TransactionCategory, []*models.TransactionTag, error) {
	fallback := unicode.UTF8.NewDecoder()
	reader := transform.NewReader(bytes.NewReader(data), unicode.BOMOverride(fallback))

	csvDataTable, err := csv.CreateNewCsvBasicDataTable(ctx, reader, false)

	if err != nil {
		return nil, nil, nil, nil, nil, nil, err
	}

	dataTable, err := createNewQianjiAppTransactionBasicDataTable(ctx, csvDataTable)

	if err != nil {
		return nil, nil, nil, nil, nil, nil, err
	}

	commonDataTable := datatable.CreateNewCommonDataTableFromBasicDataTable(dataTable)

	if !commonDataTable.HasColumn(qianjiAppTransactionTimeColumnName) ||
		!commonDataTable.HasColumn(qianjiAppTransactionTypeColumnName) ||
		!commonDataTable.HasColumn(qianjiAppTransactionSubCategoryColumnName) ||
		!commonDataTable.HasColumn(qianjiAppTransactionAccountNameColumnName) ||
		!commonDataTable.HasColumn(qianjiAppTransactionAmountColumnName) {
		log.Errorf(ctx, "[qianji_app_transaction_data_csv_file_importer.ParseImportedData] cannot parse import data, because missing essential columns in header row")
		return nil, nil, nil, nil, nil, nil, errs.ErrMissingRequiredFieldInHeaderRow
	}

	transactionDataTable, err := c.createNewqianjiAppTransactionDataTable(ctx, commonDataTable)

	if err != nil {
		return nil, nil, nil, nil, nil, nil, err
	}

	dataTableImporter := converter.CreateNewSimpleImporterWithTypeNameMapping(qianjiTransactionTypeNameMapping)

	return dataTableImporter.ParseImportedData(ctx, user, transactionDataTable, defaultTimezoneOffset, additionalOptions, accountMap, expenseCategoryMap, incomeCategoryMap, transferCategoryMap, tagMap)
}

func (c *qianjiAppTransactionDataCsvFileImporter) createNewqianjiAppTransactionDataTable(ctx core.Context, commonDataTable datatable.CommonDataTable) (datatable.TransactionDataTable, error) {
	newColumns := make([]datatable.TransactionDataTableColumn, 0, 11)
	newColumns = append(newColumns, datatable.TRANSACTION_DATA_TABLE_TRANSACTION_TYPE)
	newColumns = append(newColumns, datatable.TRANSACTION_DATA_TABLE_TRANSACTION_TIME)

	if commonDataTable.HasColumn(qianjiAppTransactionCategoryColumnName) {
		newColumns = append(newColumns, datatable.TRANSACTION_DATA_TABLE_CATEGORY)
	}

	newColumns = append(newColumns, datatable.TRANSACTION_DATA_TABLE_SUB_CATEGORY)
	newColumns = append(newColumns, datatable.TRANSACTION_DATA_TABLE_ACCOUNT_NAME)

	if commonDataTable.HasColumn(qianjiAppTransactionAccountCurrencyColumnName) {
		newColumns = append(newColumns, datatable.TRANSACTION_DATA_TABLE_ACCOUNT_CURRENCY)
	}

	newColumns = append(newColumns, datatable.TRANSACTION_DATA_TABLE_AMOUNT)
	newColumns = append(newColumns, datatable.TRANSACTION_DATA_TABLE_RELATED_ACCOUNT_NAME)

	if commonDataTable.HasColumn(qianjiAppTransactionAccountCurrencyColumnName) {
		newColumns = append(newColumns, datatable.TRANSACTION_DATA_TABLE_RELATED_ACCOUNT_CURRENCY)
	}

	newColumns = append(newColumns, datatable.TRANSACTION_DATA_TABLE_RELATED_AMOUNT)

	if commonDataTable.HasColumn(qianjiAppTransactionDescriptionColumnName) {
		newColumns = append(newColumns, datatable.TRANSACTION_DATA_TABLE_DESCRIPTION)
	}

	if commonDataTable.HasColumn(qianjiAppTransactionMemberColumnName) {
		newColumns = append(newColumns, datatable.TRANSACTION_DATA_TABLE_MEMBER)
	}

	transactionRowParser := createQianjiTransactionDataRowParser()
	transactionDataTable := datatable.CreateNewWritableTransactionDataTableWithRowParser(newColumns, transactionRowParser)
	transferTransactionsMap := make(map[string]map[datatable.TransactionDataTableColumn]string, 0)

	commonDataTableIterator := commonDataTable.DataRowIterator()

	for commonDataTableIterator.HasNext() {
		dataRow := commonDataTableIterator.Next()
		rowId := commonDataTableIterator.CurrentRowId()

		if dataRow.ColumnCount() < commonDataTable.HeaderColumnCount() {
			log.Errorf(ctx, "[qianji_app_transaction_data_csv_file_importer.createNewqianjiAppTransactionDataTable] cannot parse row \"%s\", because may missing some columns (column count %d in data row is less than header column count %d)", rowId, dataRow.ColumnCount(), commonDataTable.HeaderColumnCount())
			return nil, errs.ErrFewerFieldsInDataRowThanInHeaderRow
		}

		data := make(map[datatable.TransactionDataTableColumn]string, 11)

		for columnType, columnName := range qianjiAppDataColumnNameMapping {
			if dataRow.HasData(columnName) {
				data[columnType] = dataRow.GetData(columnName)
			}
		}

		transactionType := data[datatable.TRANSACTION_DATA_TABLE_TRANSACTION_TYPE]

		if transactionType == qianjiAppTransactionTypeIncomeText || transactionType == qianjiAppTransactionTypeExpenseText {
			switch transactionType {
			case qianjiAppTransactionTypeIncomeText:
				data[datatable.TRANSACTION_DATA_TABLE_TRANSACTION_TYPE] = qianjiTransactionTypeNameMapping[models.TRANSACTION_TYPE_INCOME]
			case qianjiAppTransactionTypeExpenseText:
				data[datatable.TRANSACTION_DATA_TABLE_TRANSACTION_TYPE] = qianjiTransactionTypeNameMapping[models.TRANSACTION_TYPE_EXPENSE]
			}

			transactionDataTable.Add(data)
		} else {
			log.Errorf(ctx, "[qianji_app_transaction_data_csv_file_importer.createNewqianjiAppTransactionDataTable] cannot parse transaction type \"%s\" in row \"%s\"", transactionType, rowId)
			return nil, errs.ErrTransactionTypeInvalid
		}
	}

	if len(transferTransactionsMap) > 0 {
		log.Errorf(ctx, "[qianji_app_transaction_data_csv_file_importer.createNewqianjiAppTransactionDataTable] there are %d transactions (related id is %s) which don't have related records", len(transferTransactionsMap), c.getqianjiAppRelatedTransactionIds(transferTransactionsMap))
		return nil, errs.ErrFoundRecordNotHasRelatedRecord
	}

	return transactionDataTable, nil
}

func (c *qianjiAppTransactionDataCsvFileImporter) getqianjiAppRelatedTransactionIds(transferTransactionsMap map[string]map[datatable.TransactionDataTableColumn]string) string {
	builder := strings.Builder{}

	for relatedId := range transferTransactionsMap {
		if builder.Len() > 0 {
			builder.WriteRune(',')
		}

		builder.WriteString(relatedId)
	}

	return builder.String()
}
