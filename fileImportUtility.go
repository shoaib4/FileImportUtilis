package v2

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	kafkaLib "bitbucket.org/infracoreplatform/kafka-lib"
	utilsKafka "bitbucket.org/infracoreplatform/proto-files/kafka/common"

	"bitbucket.org/infracoreplatform/kafka-lib/keeper"
	"bitbucket.org/infracoreplatform/kafka-lib/publisher"
	awsUtils "bitbucket.org/infracoreplatform/server-utils/aws"
	"bitbucket.org/infracoreplatform/server-utils/common"
	excelize "github.com/xuri/excelize/v2"
	"gopkg.in/guregu/null.v3"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type FileImportAuditUtility struct {
	S3             awsUtils.S3Instance
	KafkaProducer  publisher.IKafkaPublisher
	Db             *gorm.DB
	SourceService  string
	OutboxTable    string
	ProducerConfig kafkaLib.KafkaPublisherConfig
	Logger         common.LogglyLoggerInterface
}

type KafkaMessageConfig struct {
	EventType     string
	EventSubType  string
	TransactionId string
	RequestId     string
}
type FileImportAudit struct {
	gorm.Model
	FileName     string   `gorm:"column:file_name"`
	InputUrl     string   `gorm:"column:input_url"`
	OutputUrl    string   `gorm:"column:output_url"`
	Status       string   `gorm:"column:status"`
	ParentId     null.Int `gorm:"column:parent_id"`
	Tag          string   `gorm:"column:tag"`
	ErrorMessage string   `gorm:"column:error_message"`
	Info         []byte   `gorm:"column:info;default:{}"`
	CreatedBy    int      `gorm:"column:created_by"`
	UpdatedBy    int      `gorm:"column:updated_by"`
}

const (
	FileStatusNew          = "uploaded"
	FileStatusProgress     = "processing"
	FileStatusProcessed    = "processed"
	FileStatusSchemaFailed = "schema_failed"
	FileStatusFailed       = "failed"
	FileStatusPartial      = "partial"
	RecordFailedStatus     = "failed"
	RecordSuccessStatus    = "success"
	ExcelStatusColumnName  = "status"
	CsvContentType         = "csv"
	ProcessedOffset        = "processed_offset"
)

var (
	responseFileSuffix    = "_Response"
	batchFilesChildFolder = "child/"
	defaultSheetName      = "Sheet1"
	s3Protocol            = "https://"
	s3RegionArn           = ".s3.ap-south-1.amazonaws.com"
	fileStatusRemarkMap   = map[string]string{
		FileStatusSchemaFailed: "Failed to upload the file. Please verify the file and upload it again.",
		FileStatusFailed:       "All records have failed to update. Please download to see the reasons for the failure.",
		FileStatusProcessed:    "All records are updated successfully.",
		FileStatusPartial:      "Several records failed to update. For more information about the failure, please download.",
		FileStatusNew:          "File uploaded",
		FileStatusProgress:     "File processing",
	}
	fileStatusTerminalStates = []string{FileStatusPartial, FileStatusProcessed, FileStatusFailed}
)

type IFileImportAudit interface {
	UploadInputFileData(bucket string, filePath string, fileData []byte, sheetName string, tag string, userId int64, excelBatchSize int, kafkaMessageConfig KafkaMessageConfig, schemaValidation func(fileData [][]string) (bool, error)) (string, error)
	GetFileData(bucket string, sheetName string, auditEntryId int) (fileData [][]string, url string, err error)
	GetCSVFileData(bucket string, auditEntryId int) (result [][]string, url string, err error)
	UploadCsvFileData(bucker string, rows [][]string, filePath string) error
	UploadOutputFileData(bucket string, rows [][]string, sheetName string, outputFilePath string, auditEntryId int) error
	GetFileImportAudit(cursor int, pageSize int, orderBy string, tag string) ([]FileImportAudit, int64, error)
	UpdateAuditEntry(auditEntryId int, updates map[string]interface{}) (FileImportAudit, error)
	GetFileImportAuditV1(cursor int, pageSize int, orderBy string, tagArray []string, orderType string, statusArray []string, createdAt string, updatedAt string) ([]FileImportAudit, int64, error)
	GetFileDataMultiSheet(bucket string, sheetNames []string, auditEntryId int) (res [][][]string, url string, err error)
	UploadInputFileDataMultiSheet(bucket string, filePath string, fileData []byte, sheetNames []string, tag string, userId int64, kafkaMessageConfig KafkaMessageConfig, schemaValidationMap map[string]func(fileData [][]string) (bool, error)) (string, error)
	UploadOutputFileDataMultiSheet(bucket string, rows [][][]string, sheetNames []string, outputFilePath string, auditEntryId int, sheetNameForFileStatus string) error
	ChunkCSVUploadAndNotify(bucket string, auditEntryId int, tag string, userId int64, fileByteSize int64, kafkaMessageConfig KafkaMessageConfig, headerPresent bool)
	ProcessCSVDataWithFunction(bucket string, inputURL string, fileByteSize int64, dataProcessorFunction func(CsvProcessingWithFunctionInput) (interface{}, error), options ...func(c *CsvProcessingConfig)) []CsvProcessingWithFunctionResult
	GetChildFileAuditEntries(auditEntryId int) ([]FileImportAudit, error)
	ValidateFileImportData(bucket string, inputURL string, validations ...func(*validationHelper) error) error
}

func getFileStatus(rows [][]string) string {
	var statusIndex int
	for index, item := range rows[0] {
		if strings.ToLower(item) == ExcelStatusColumnName {
			statusIndex = index
		}
	}

	failRecordPresent := false
	successRecordPresent := false

	for _, items := range rows {
		if strings.ToLower(items[statusIndex]) == RecordFailedStatus {
			failRecordPresent = true
		} else if strings.ToLower(items[statusIndex]) == RecordSuccessStatus {
			successRecordPresent = true
		}
	}

	if failRecordPresent && !successRecordPresent {
		return FileStatusFailed
	} else if successRecordPresent && failRecordPresent {
		return FileStatusPartial
	} else {
		return FileStatusProcessed
	}
}

func readFromExcel(sheetName string, fileBytes []byte, logger common.LogglyLoggerInterface) ([][]string, error) {
	excelFile, excelReadErr := excelize.OpenReader(bytes.NewReader(fileBytes))
	if excelReadErr != nil {
		logger.WithField("err", excelReadErr).Error("Error in reading excel file with excelize")
		return nil, excelReadErr
	}
	rows, err := excelFile.Rows(sheetName)
	if err != nil {
		logger.WithField("error_reading_rows", err).Error("Error reading rows from excel")
		return [][]string{}, err
	}
	var excelRows [][]string
	isHeader := true
	headerLen := 0
	for rows.Next() {
		row, err := rows.Columns()
		if err != nil {
			logger.WithField("error_reading_cols", err).Error("Error reading col values for row")
			return [][]string{}, err
		}
		var rowTemp []string
		rowTemp = append(rowTemp, row...)
		if isHeader {
			headerLen = len(rowTemp)
			isHeader = false
		}
		//make row length same as header len if col values missing in right side
		if len(rowTemp) < headerLen {
			rowTemp = append(rowTemp, make([]string, headerLen-len(rowTemp))...)
		}
		excelRows = append(excelRows, rowTemp)
	}
	err = rows.Close()
	if err != nil {
		logger.WithField("fileCloseError", err).Error("Error closing read file object")
	}
	return excelRows, nil
}

func writeToExcel(data [][][]string, sheetNames []string, logger common.LogglyLoggerInterface) (*bytes.Buffer, error) {
	f := excelize.NewFile()
	defer func() {
		if err := f.Close(); err != nil {
			logger.WithField("fileCloseError", err).Error("Error closing excel")
			return
		}
	}()
	deleteDefaultSheet := true
	for index, sheetName := range sheetNames {
		if sheetName == defaultSheetName {
			deleteDefaultSheet = false
		}
		f.NewSheet(sheetName)
		rows := data[index]
		sw, err := f.NewStreamWriter(sheetName)
		if err != nil {
			logger.WithField("streamWriterCreateError", err).Error("Error creating stream writer")
			return nil, err
		}
		for idx, row := range rows {
			cell, err := excelize.CoordinatesToCellName(1, idx+1)
			if err != nil {
				logger.WithField("excelWriteError", err).Error("Error writing to excel")
				return nil, err
			}
			var rowInterface []interface{}
			for _, val := range row {
				rowInterface = append(rowInterface, val)
			}
			err = sw.SetRow(cell, rowInterface)
			if err != nil {
				logger.WithField("excelRowWriteError", err).Error("Error writing row to excel")
				return nil, err
			}
		}
		if err := sw.Flush(); err != nil {
			logger.WithField("flushError", err).Error("Error in flushing stream writer")
			return nil, err
		}
	}
	if deleteDefaultSheet {
		f.DeleteSheet(defaultSheetName)
	}
	buffer, err := f.WriteToBuffer()
	if err != nil {
		logger.WithField("excelFileToBufferWriteError", err).Error("Error writing file to buffer")
		return nil, err
	}
	err = f.Close()
	if err != nil {
		logger.WithField("fileCloseError", err).Error("Error closing write file object")
	}
	return buffer, nil
}

func getAuditEntry(fileName string, completeS3Path string, tag string, userId int64, parentId int64) FileImportAudit {
	flt := FileImportAudit{
		FileName:  fileName,
		InputUrl:  completeS3Path,
		Status:    FileStatusNew,
		Tag:       tag,
		CreatedBy: int(userId),
		UpdatedBy: int(userId),
		ParentId:  null.IntFrom(parentId),
	}
	return flt
}

func (fia *FileImportAuditUtility) setKafkaProducer() {
	kafkaProducer := publisher.NewKafkaPublisher(fia.ProducerConfig, fia.Logger)
	fia.KafkaProducer = &kafkaProducer
}

func (fia *FileImportAuditUtility) sendEventToKafka(kafkaMessageConfig KafkaMessageConfig, userId int64, fileImportAuditId int64, parentId int64, tx *gorm.DB) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = errors.New(fmt.Sprintf("panic occurred in send event, recovered value: %v", rec))
		}
	}()
	transactionSuffix := ""
	if parentId != 0 {
		transactionSuffix = strconv.Itoa(int(fileImportAuditId))
	}
	err = fia.KafkaProducer.SendEventWithPersistence(&keeper.GormKeeperTx{
		Db: tx,
	}, kafkaLib.KafkaPayload{
		BasePayload: kafkaLib.BasePayload{
			CreatedAtTs:  time.Now(),
			ModifiedAtTs: time.Now(),
		},
		SourceService: fia.SourceService,
		TransactionId: kafkaMessageConfig.TransactionId + transactionSuffix,
		RequestId:     kafkaMessageConfig.RequestId,
		Type:          kafkaMessageConfig.EventType,
		SubType:       kafkaMessageConfig.EventSubType,
		Payload: &utilsKafka.FileImportData{
			UserId:            userId,
			FileImportAuditId: fileImportAuditId,
			ParentId:          parentId,
		},
		DBTable: fia.OutboxTable,
	}, fia.Logger)
	return err
}

func breakRecordsInBatches(arr [][]string, batchSize int) [][][]string {
	header := arr[0] // extract header row
	arr = arr[1:]
	var batches [][][]string
	for batchSize < len(arr) {
		arr, batches = arr[batchSize:], append(batches, append([][]string{header}, arr[0:batchSize]...))
	}
	return append(batches, append([][]string{header}, arr...))
}

func (fia *FileImportAuditUtility) breakExcelBatchesToByteArr(rows [][]string, excelBatchSize int, sheetName string) (byteArr [][]byte) {
	batches := breakRecordsInBatches(rows, excelBatchSize)
	for _, batch := range batches {
		excelBytes, err := writeToExcel([][][]string{batch}, []string{sheetName}, fia.Logger)
		if err != nil {
			return nil
		}
		byteArr = append(byteArr, excelBytes.Bytes())
	}
	return byteArr
}

func (fia *FileImportAuditUtility) addChildAuditEntryAndSendToKafka(parentFileAuditId int, childUrl string, parentAuditMetaData map[string]string, kafkaMessageConfig KafkaMessageConfig, tag string, userId int64) error {
	tx := fia.Db.Begin()
	defer tx.Rollback()
	fileName := childUrl[strings.LastIndex(childUrl, "/")+1:]
	flt := getAuditEntry(fileName, childUrl, tag, userId, int64(parentFileAuditId))
	result := tx.Create(&flt)
	if result.Error != nil {
		return result.Error
	}
	err := fia.sendEventToKafka(kafkaMessageConfig, userId, int64(flt.ID), int64(parentFileAuditId), tx)
	if err != nil {
		return err
	}
	parentInfoJson, err := json.Marshal(parentAuditMetaData)
	if err != nil {
		return err
	}
	_, err = fia.UpdateAuditEntry(parentFileAuditId, map[string]interface{}{"info": parentInfoJson})
	if err != nil {
		return err
	}
	tx.Commit()
	return nil
}

func (fia *FileImportAuditUtility) addAuditEntryAndSendToKafka(outputUrls []string, kafkaMessageConfig KafkaMessageConfig, tag string, userId int64) error {
	tx := fia.Db.Begin()
	defer tx.Rollback()
	flt := getAuditEntry(outputUrls[0][strings.LastIndex(outputUrls[0], "/")+1:], outputUrls[0], tag, userId, 0)
	result := tx.Create(&flt)
	if result.Error != nil {
		fia.Logger.WithField("getAuditEntryError", result.Error).Error("Error getting audit entry for kafka event")
		return result.Error
	}
	if len(outputUrls) == 1 {
		err := fia.sendEventToKafka(kafkaMessageConfig, userId, int64(flt.ID), 0, tx)
		if err != nil {
			return err
		}
	} else {
		parentId := flt.ID
		var childFileAuditIds []int64
		for _, outputUrl := range outputUrls[1:] {
			fileName := outputUrl[strings.LastIndex(outputUrl, "/")+1:]
			flt = getAuditEntry(fileName, outputUrl, tag, userId, int64(parentId))
			result = tx.Create(&flt)
			if result.Error != nil {
				return result.Error
			}
			childFileAuditIds = append(childFileAuditIds, int64(flt.ID))
		}
		for _, auditId := range childFileAuditIds {
			err := fia.sendEventToKafka(kafkaMessageConfig, userId, auditId, int64(parentId), tx)
			if err != nil {
				return err
			}
		}
	}
	tx.Commit()
	return nil
}

func (fia *FileImportAuditUtility) addSchemaFailedAuditEntry(baseUrl string, filePath string, fileName string, tag string, userId int64, schemaValidationErr error) (err error) {
	flt := getAuditEntry(fileName, baseUrl+filePath, tag, userId, 0)
	flt.ErrorMessage = schemaValidationErr.Error()
	flt.Status = FileStatusSchemaFailed
	flt.ErrorMessage = fileStatusRemarkMap[flt.Status]
	result := fia.Db.Create(&flt)
	return result.Error
}

func (fia *FileImportAuditUtility) handleBatchCompleteEvent(auditEntryId int, bucket string, sheetName string) (err error) {
	var flt FileImportAudit
	result := fia.Db.Model(&FileImportAudit{}).Where("id = ?", auditEntryId).First(&flt)
	if result.Error != nil {
		fia.Logger.WithField("err", result.Error).Error("Error reading parent record in HandleBatchCompleteEvent")
		return err
	}
	if flt.ParentId.Int64 == 0 {
		return nil
	}
	parentId := flt.ParentId.Int64
	var count int64
	result = fia.Db.Model(&FileImportAudit{}).Where("status not in ? and parent_id = ?", fileStatusTerminalStates, parentId).Count(&count)
	if result.Error != nil {
		fia.Logger.WithField("err", result.Error).Error("error reading child records in HandleBatchCompleteEvent")
		return result.Error
	}
	//if all child files have been processed
	if count == 0 {
		excelJoinedRecords, err := fia.getJoinedRecords(int(parentId), bucket, sheetName)
		if err != nil {
			fia.Logger.WithField("err", err).Error("error in joining batch results in HandleBatchCompleteEvent")
			return err
		}

		tx := fia.Db.Begin()
		defer tx.Rollback()
		result = tx.Model(&FileImportAudit{}).Where("id = ?", parentId).Scan(&flt)
		if result.Error != nil {
			fia.Logger.WithField("err", result.Error).Error("error fetching master record from db in HandleBatchCompleteEvent")
			return err
		}
		//upload combined results on s3
		s3BaseUrl, fileName, fileFolder, extension := getPathComponents(bucket, strings.TrimPrefix(flt.InputUrl, s3Protocol+bucket+s3RegionArn))
		masterFilePath := fileFolder + fileName + responseFileSuffix + extension
		buff, err := writeToExcel([][][]string{excelJoinedRecords}, []string{sheetName}, fia.Logger)
		if err != nil {
			fia.Logger.WithField("err", err).Error("Error while writing final output to excel")
			return err
		}
		_, err = fia.S3.Fileupload(bucket, base64.StdEncoding.EncodeToString(buff.Bytes()), masterFilePath, extension[1:], fia.Logger.GetLogrusEntry())
		if err != nil {
			fia.Logger.WithField("err", err).Error("Error uploading master file to s3")
			return err
		}
		//delete child records and update output url in parent
		var fltArr []FileImportAudit
		result = tx.Model(&FileImportAudit{}).Select("*").Where("parent_id = ?", parentId).Scan(&fltArr)
		if result.Error != nil {
			fia.Logger.WithField("err", result.Error).Error("Error reading child records for given parent id")
			return err
		}
		tx.Delete(&fltArr)
		masterFileStatus := findMasterFileStatus(fltArr)
		result = tx.Model(&flt).Updates(map[string]interface{}{"status": masterFileStatus, "output_url": s3BaseUrl + masterFilePath, "error_message": fileStatusRemarkMap[masterFileStatus]})
		if result.Error != nil {
			fia.Logger.WithField("err", result.Error).Error("error updating master audit entry")
			return err
		}
		tx.Commit()
	}
	return nil
}

func findMasterFileStatus(arr []FileImportAudit) string {
	failed := 0
	for _, flt := range arr {
		if flt.Status == FileStatusPartial {
			return FileStatusPartial
		}
		if flt.Status == FileStatusFailed {
			failed += 1
		}
	}
	if failed == len(arr) {
		return FileStatusFailed
	}
	if failed > 0 {
		return FileStatusPartial
	}
	return FileStatusProcessed
}

func (fia *FileImportAuditUtility) getJoinedRecords(parentId int, bucket string, sheetName string) ([][]string, error) {
	var fltArr []FileImportAudit
	result := fia.Db.Model(&FileImportAudit{}).Where("parent_id = ?", parentId).Order("id asc").Scan(&fltArr)
	if result.Error != nil {
		return [][]string{}, result.Error
	}
	var excelJoinedRecords [][]string
	for index, fileAudit := range fltArr {
		childRecords, err := fia.downloadFileAndGetRows(bucket, sheetName, fileAudit.OutputUrl)
		if err != nil {
			return [][]string{}, err
		}
		if index != 0 {
			childRecords = childRecords[1:] //avoid header
		}
		excelJoinedRecords = append(excelJoinedRecords, childRecords...)
	}
	return excelJoinedRecords, nil
}

func getPathComponents(bucket string, filePath string) (string, string, string, string) {
	s3BaseUrl := s3Protocol + bucket + s3RegionArn
	fileFolder := filePath[0 : strings.LastIndex(filePath, "/")+1]
	fileName := filePath[strings.LastIndex(filePath, "/")+1:]
	extension := fileName[strings.LastIndex(fileName, "."):]
	fileName = fileName[0:strings.LastIndex(fileName, ".")]
	return s3BaseUrl, fileName, fileFolder, extension
}

func (fia *FileImportAuditUtility) uploadOutputAndUpdateAuditEntry(bucket string, fileStatus string, auditEntryId int, outputFilePath string, buff *bytes.Buffer, baseUrl string) error {
	var fileImportAudit FileImportAudit
	result := fia.Db.Where("id = ?", auditEntryId).First(&fileImportAudit)
	if result.Error != nil {
		fia.Logger.WithField("err", result.Error).Error("Could not fetch audit entry")
		return result.Error
	}
	extension := outputFilePath[strings.LastIndex(outputFilePath, ".")+1:]
	_, err := fia.S3.Fileupload(bucket, base64.StdEncoding.EncodeToString(buff.Bytes()), outputFilePath, extension, fia.Logger.GetLogrusEntry())
	if err != nil {
		fia.Logger.WithField("err", err).Error("error uploading output file UploadOutputFileData")
		return err
	}
	outputFileUrl := baseUrl + outputFilePath
	errorMessage := fileStatusRemarkMap[fileStatus]
	_, err = fia.UpdateAuditEntry(auditEntryId, map[string]interface{}{"status": fileStatus, "output_url": outputFileUrl, "error_message": errorMessage})
	return err
}

func (fia *FileImportAuditUtility) downloadFileAndGetRows(bucket string, sheetName string, s3FileUrl string) ([][]string, error) {
	folderPath := strings.TrimPrefix(s3FileUrl, s3Protocol+bucket+s3RegionArn)
	base64encoding, _, err := fia.S3.FileDownload(bucket, folderPath, fia.Logger.GetLogrusEntry())
	if err != nil {
		fia.Logger.WithField("err", err).Error("error downloading s3 file")
		return [][]string{}, err
	}
	fileBytes, err := base64.StdEncoding.DecodeString(base64encoding)
	if err != nil {
		fia.Logger.WithField("err", err).Error("Could not convert base64 to bytes")
		return [][]string{}, err
	}
	return readFromExcel(sheetName, fileBytes, fia.Logger)
}

func (fia *FileImportAuditUtility) GetFileImportAudit(cursor int, pageSize int, orderBy string, tag string) ([]FileImportAudit, int64, error) {
	fia.Logger.Info("Started repo level operations to GetFileImportAudit from db")
	var fileAudit []FileImportAudit
	var rowCount int64

	result := fia.Db.Model(&FileImportAudit{}).Offset(cursor).Limit(pageSize).Order("created_at " + orderBy)

	if tag != "" {
		result.Where("tag = ?", tag)
		fia.Db.Model(&FileImportAudit{}).Where("tag = ?", tag).Count(&rowCount)
	} else {
		fia.Db.Model(&FileImportAudit{}).Count(&rowCount)
	}
	result.Find(&fileAudit)
	if result.Error != nil {
		fia.Logger.WithField("err", result.Error.Error()).Error("Error while GetFileImportAudit from db")
		return nil, 0, result.Error
	}
	return fileAudit, rowCount, nil
}

func (fia FileImportAuditUtility) GetFileImportAuditV1(cursor int, pageSize int, orderBy string, tagArray []string, orderType string, statusArray []string, startDate string, endDate string) ([]FileImportAudit, int64, error) {
	fia.Logger.Info("Started repo level operations to GetFileImportAuditV1 from db")
	var fileAudit []FileImportAudit
	var rowCount int64
	result := fia.Db.Model(&FileImportAudit{})

	if len(tagArray) > 0 {
		result.Where("tag IN ? ", tagArray)
	}
	if len(statusArray) > 0 {
		result.Where("status IN ?", statusArray)
	}
	if strings.TrimSpace(startDate) != "" {
		result.Where("extract(EPOCH FROM created_at) >=?", startDate)
	}
	if strings.TrimSpace(endDate) != "" {
		result.Where("extract(EPOCH FROM created_at) <= ?", endDate)
	}
	result.Count(&rowCount).Offset(cursor).Limit(pageSize).Order(orderType + " " + orderBy)

	fia.Logger.Info("Final query ", result)
	result.Find(&fileAudit)
	if result.Error != nil {
		fia.Logger.WithField("err", result.Error.Error()).Error("Error while GetFileImportAuditV1 from db")
		return nil, 0, result.Error
	}
	return fileAudit, rowCount, nil
}

func (fia *FileImportAuditUtility) UpdateAuditEntry(auditEntryId int, updates map[string]interface{}) (FileImportAudit, error) {
	var fileImportAudit FileImportAudit
	result := fia.Db.Model(&fileImportAudit).Clauses(clause.Returning{}).Where("id = ?", auditEntryId).Updates(updates)
	if result.Error != nil {
		fia.Logger.WithField("err", result.Error).Error("Could not update audit entry with given updates")
		return FileImportAudit{}, result.Error
	}
	return fileImportAudit, nil
}

func (fia *FileImportAuditUtility) UploadCsvFileData(bucket string, rows [][]string, filePath string) error {
	_, _, _, extension := getPathComponents(bucket, filePath)
	data, err := convertCSVToStrings(rows)
	if err != nil {
		fia.Logger.WithField("err", err).Error("Error uploading file to s3")
		return err
	}
	_, err = fia.S3.Fileupload(bucket, data, filePath, extension[1:], fia.Logger.GetLogrusEntry())
	if err != nil {
		fia.Logger.WithField("err", err).Error("Error uploading file to s3")
		return err
	}
	return nil
}

func (fia *FileImportAuditUtility) UploadInputFileData(bucket string, filePath string, fileData []byte, sheetName string, tag string, userId int64, excelBatchSize int,
	kafkaMessageConfig KafkaMessageConfig,
	schemaValidation func(fileData [][]string) (bool, error)) (string, error) {
	fia.setKafkaProducer()
	s3BaseUrl, fileName, fileFolder, extension := getPathComponents(bucket, filePath)
	_, err := fia.S3.Fileupload(bucket, base64.StdEncoding.EncodeToString(fileData), filePath, extension[1:], fia.Logger.GetLogrusEntry())
	if err != nil {
		fia.Logger.WithField("err", err).Error("Error uploading file to s3")
		return "Failed", err
	}
	excelRows, err := readFromExcel(sheetName, fileData, fia.Logger)
	if err != nil {
		fia.Logger.WithField("err", err).Error("Error in excel processing")
		return "Failed", err
	}
	isSchemaValid, err := schemaValidation(excelRows)
	if !isSchemaValid || err != nil {
		err := fia.addSchemaFailedAuditEntry(s3BaseUrl, filePath, fileName+extension, tag, userId, err)
		if err != nil {
			fia.Logger.WithField("err", err).Error("Error adding excel processing")
			return "Failed", err
		}
		return "Failed", errors.New("Excel file has invalid schema")
	}
	outputUrls := []string{s3BaseUrl + filePath}
	if excelBatchSize > 0 && len(excelRows) > excelBatchSize {
		byteArr2d := fia.breakExcelBatchesToByteArr(excelRows, excelBatchSize, sheetName)
		for index, fileByte := range byteArr2d {
			childFilePath := fileFolder + batchFilesChildFolder + fileName + "_" + strconv.Itoa(index) + extension
			_, err := fia.S3.Fileupload(bucket, base64.StdEncoding.EncodeToString(fileByte), childFilePath, extension[1:], fia.Logger.GetLogrusEntry())
			if err != nil {
				fia.Logger.WithField("err", err).Error("Error in uploading child file")
				return "Failed", err
			}
			outputUrls = append(outputUrls, s3BaseUrl+childFilePath)
		}
	}
	err = fia.addAuditEntryAndSendToKafka(outputUrls, kafkaMessageConfig, tag, userId)
	if err != nil {
		fia.Logger.WithField("err", err).Error("Error while completing transaction for audit entry and kafka send event")
		return "Failed", err
	}
	return "Success", nil
}
func convertCSVToStrings(data [][]string) (string, error) {
	buf := new(bytes.Buffer)
	writer := csv.NewWriter(buf)
	for _, record := range data {
		if err := writer.Write(record); err != nil {
			return "", err
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return "", err
	}
	base64Str := base64.StdEncoding.EncodeToString(buf.Bytes())
	return base64Str, nil

}

func convertSliceToMatrixCSV(data []string) ([][]string, error) {
	var matrix [][]string
	for _, row := range data {
		reader := csv.NewReader(strings.NewReader(row))
		record, err := reader.Read()
		if err != nil {
			return nil, err // Handle the error appropriately
		}
		matrix = append(matrix, record)
	}
	return matrix, nil
}

func readAllCSVWithPartialData(data string, fileEnded bool) ([][]string, string, error) {
	rows := strings.Split(data, "\n")
	var partialRow string
	if len(rows) > 0 {
		if fileEnded {
			partialRow = rows[len(rows)-1]
			if len(partialRow) == 0 {
				rows = rows[:len(rows)-1]
			}
		} else {
			partialRow = rows[len(rows)-1]
			rows = rows[:len(rows)-1]
		}
	}
	matData, err := convertSliceToMatrixCSV(rows)
	return matData, partialRow, err

}

func readDataFromCsvReader(reader *csv.Reader) ([][]string, error) {
	var batch [][]string
	for {
		record, err := reader.Read()
		if err == io.EOF {
			return batch, nil
		}
		if err != nil {
			return batch, err
		}
		batch = append(batch, record)
	}
	return batch, nil
}

func (fia *FileImportAuditUtility) GetChildFileAuditEntries(auditEntryId int) ([]FileImportAudit, error) {
	var fileImportAudit []FileImportAudit
	result := fia.Db.Model(&fileImportAudit).Where("parent_id = ?", auditEntryId).Find(&fileImportAudit)
	if result.Error != nil {
		fia.Logger.WithField("err", result.Error).Error("Could not get child entries for the file audit entry with id = ", auditEntryId)
		return make([]FileImportAudit, 0), result.Error
	}
	return fileImportAudit, nil
}

func (fia *FileImportAuditUtility) updateParentAuditEntryStatusFailed(auditEntryId int, errorMessage string) error {
	childFileAuditEntries, err := fia.GetChildFileAuditEntries(auditEntryId)
	if len(childFileAuditEntries) == 0 {
		_, err = fia.UpdateAuditEntry(auditEntryId, map[string]interface{}{"status": FileStatusFailed, "error_message": errorMessage})
	} else {
		_, err = fia.UpdateAuditEntry(auditEntryId, map[string]interface{}{"status": FileStatusPartial, "error_message": errorMessage})
	}
	if err != nil {
		return err
	}
	return nil
}

// s3OperationsForChunkCSVUploadAndNotify function handles chunk-wise uploading of processed CSV data back to an S3 bucket
// and associated notification. It takes the processed data from the ProcessCSVDataWithFunction function, uploads each
// chunk to the designated S3 location, and notifies about the completion of each upload. Key features of this function
// This function operates concurrently with dbOperationschunkCSVUploadAndNotify, sharing state via channels.
// It employs a context to manage cancellation signals across goroutines, ensuring graceful shutdown on errors or completion.
func (fia *FileImportAuditUtility) s3OperationsForChunkCSVUploadAndNotify(ctx context.Context, cancel context.CancelFunc, bucket string,
	fileByteSize int64, fileLength int64, inputUrl string, headerPresent bool, s3BatchUpdateChan chan s3BatchChanData, s3BatchErrorChan chan error) {
	defer func() {
		if rec := recover(); rec != nil {
			s3BatchErrorChan <- fmt.Errorf("uploading to s3 failed with error ", rec)
		}
	}()
	_ = fia.ProcessCSVDataWithFunction(bucket, inputUrl, fileByteSize, func(inputData CsvProcessingWithFunctionInput) (interface{}, error) {
		var err error
		start := time.Now()
		s3BaseUrl, fileName, fileFolder, extension := getPathComponents(bucket, inputUrl)
		csvDataArray := inputData.Data
		index := inputData.MetaData.Index
		fia.Logger.Info("Started processing ChunkCSVUploadAndNotify/s3OperationsForChunkCSVUploadAndNotify  Chunk ", index)
		childFileURL := fileFolder + batchFilesChildFolder + fileName + "_" + strconv.Itoa(index) + extension
		fileData, _ := convertCSVToStrings(csvDataArray)
		childFilePath := strings.TrimPrefix(childFileURL, s3BaseUrl)
		_, err = fia.S3.Fileupload(bucket, fileData, childFilePath, extension[1:], fia.Logger.GetLogrusEntry())
		if err != nil {
			fia.Logger.WithField("err", err).Error("Error in uploading child file")
			s3BatchErrorChan <- err
			return nil, err
		}
		parentInfoMap := map[string]string{
			ProcessedOffset: strconv.FormatInt(inputData.MetaData.Offset, 10),
		}
		s3BatchUpdateChan <- s3BatchChanData{ChildUrl: childFileURL, ParentInfoMap: parentInfoMap}
		duration := time.Since(start)
		fia.Logger.Info("Ended processing ChunkCSVUploadAndNotify/s3OperationsForChunkCSVUploadAndNotify  Chunk ", index-1, " in ", duration)
		return nil, err
	}, SetHeaderPresent(headerPresent))
	cancel()
}

// dbOperationschunkCSVUploadAndNotify handles database updates based on the results of the S3 chunk processing.
// It listens for processed chunk information through a channel and updates database entries accordingly.
// Additionally, it sends a message to Kafka to notify other systems of the update.
// This function runs concurrently with s3OperationsForChunkCSVUploadAndNotify and uses a shared context for cancellation.
// It stops on receiving an error, a cancellation signal, or after completing all updates.
func (fia *FileImportAuditUtility) dbOperationschunkCSVUploadAndNotify(ctx context.Context, auditEntryId int, tag string, userId int64,
	kafkaMessageConfig KafkaMessageConfig, s3BatchUpdateChan chan s3BatchChanData, s3BatchErrorChan chan error, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case updateData := <-s3BatchUpdateChan:
			err := fia.addChildAuditEntryAndSendToKafka(auditEntryId, updateData.ChildUrl, updateData.ParentInfoMap, kafkaMessageConfig, tag, userId)
			if err != nil {
				_ = fia.updateParentAuditEntryStatusFailed(auditEntryId, err.Error())
				fia.Logger.WithField("err", err).Error("Error while completing transaction for audit entry and kafka send event in ChunkCSVUploadAndNotify/s3OperationsForChunkCSVUploadAndNotify")
				return
			}
		case err := <-s3BatchErrorChan:
			_ = fia.updateParentAuditEntryStatusFailed(auditEntryId, err.Error())
			fia.Logger.WithField("err", err).Error("Error while completing transaction for audit entry and kafka send event in ChunkCSVUploadAndNotify/s3OperationsForChunkCSVUploadAndNotify")
			return
		case <-ctx.Done():
			fia.Logger.Info("Ended the ChunkCSVUploadAndNotify from ctx.Done in dbOperationschunkCSVUploadAndNotify")
			_, _ = fia.UpdateAuditEntry(auditEntryId, map[string]interface{}{"status": FileStatusProcessed})
			return
		}
	}
}

type s3BatchChanData struct {
	ChildUrl      string
	ParentInfoMap map[string]string
}

// ChunkCSVUploadAndNotify orchestrates the process of handling large CSV files stored in S3 by breaking them down into manageable chunks,
// uploading these chunks back to S3, updating a database table with entries for these chunks, and sending notifications via Kafka.
// This function sets up the necessary context and channels for concurrent operations, initiates streaming data from S3,
// and handles database operations based on the chunk processing results.
// It ensures the file processed is of CSV type and initiates goroutines for S3 operations and database updates.
//
// Parameters:
//   - bucket: The S3 bucket where the original file is stored.
//   - auditEntryId: Identifier for the file audit entry for the original file in the database.
//   - tag: To enter tag to the file import audit table.
//   - userId: Identifier for the user initiating the process.
//   - fileByteSize: The target size for each chunk to be processed. This size is not guaranteed to be exact,
//     as the processing logic ensures that chunks contain complete CSV rows. The actual size of each processed chunk
//     may vary slightly to accommodate the inclusion of whole rows without cutting off data mid-row.
//   - kafkaMessageConfig: Configuration details for sending Kafka messages.
//   - headerPresent: Flag indicating whether the CSV file includes a header row.
func (fia *FileImportAuditUtility) ChunkCSVUploadAndNotify(bucket string, auditEntryId int, tag string, userId int64,
	fileByteSize int64, kafkaMessageConfig KafkaMessageConfig, headerPresent bool) {
	fia.setKafkaProducer()
	fileImportAuditData, err := fia.UpdateAuditEntry(auditEntryId, map[string]interface{}{"status": FileStatusProgress})
	if err != nil {
		return
	}
	s3BaseUrl, _, _, _ := getPathComponents(bucket, fileImportAuditData.InputUrl)
	filePath := strings.TrimPrefix(fileImportAuditData.InputUrl, s3BaseUrl)
	fileLenght, fileContentType, _ := fia.S3.GetFileLengthAndContentType(bucket, filePath, fia.Logger.GetLogrusEntry())
	if fileContentType != CsvContentType {
		err = errors.New("file type is not CSV")
		_ = fia.updateParentAuditEntryStatusFailed(auditEntryId, err.Error())
		return
	}

	ctx, cancel := context.WithCancel(context.Background())

	s3BatchUpdateChan := make(chan s3BatchChanData)
	s3BatchErrorChan := make(chan error)
	var wg sync.WaitGroup
	go fia.s3OperationsForChunkCSVUploadAndNotify(ctx, cancel, bucket, fileByteSize, fileLenght, fileImportAuditData.InputUrl, headerPresent,
		s3BatchUpdateChan, s3BatchErrorChan)
	wg.Add(1)
	go fia.dbOperationschunkCSVUploadAndNotify(ctx, auditEntryId, tag, userId, kafkaMessageConfig, s3BatchUpdateChan, s3BatchErrorChan, &wg)
	wg.Wait()
}

type CsvProcessingConfig struct {
	HeaderPresent bool
	BreakOnError  bool
	ProcessOnce   bool
}
type CsvProcessingWithFunctionResult struct {
	Data interface{}
	Err  error
}
type CsvProcessingWithFunctionInputMetaData struct {
	Offset int64
	Index  int
}
type CsvProcessingWithFunctionInput struct {
	Data     [][]string
	MetaData CsvProcessingWithFunctionInputMetaData
}

func SetHeaderPresent(val bool) func(*CsvProcessingConfig) {
	return func(obj *CsvProcessingConfig) {
		obj.HeaderPresent = val
	}
}

func SetBreakOnError(val bool) func(*CsvProcessingConfig) {
	return func(obj *CsvProcessingConfig) {
		obj.BreakOnError = val
	}
}

func SetProcessOnce(val bool) func(*CsvProcessingConfig) {
	return func(obj *CsvProcessingConfig) {
		obj.ProcessOnce = val
	}
}

// ProcessCSVDataWithFunction processes a CSV file stored in an S3 bucket in chunks and applies the provided function to each chunk.
// It returns the results of applying the function on each chunk along with any encountered errors.
// The function signature for the processing function is func([][]string) (interface{}, error).
// Optional configurations can be applied using functional options pattern on CsvProcessingWithFuncOptionsObj object.
// The function accepts the following parameters:
//   - bucket: the name of the S3 bucket where the CSV file is stored.
//   - inputURL: the URL of the CSV file within the S3 bucket.
//   - fileByteSize: the maximum byte size of each chunk to download and process.
//   - dataProcessorFunction: the function to apply to each chunk of CSV data.
//   - CsvProcessingWithFunctionInput represents the input data structure for the processing function. It contains two fields:
//   - Data: a 2D slice representing the CSV data chunk.
//   - MetaData: contains metadata information about the chunk, including Offset (the byte offset of the chunk within the file)
//     and Index (the index of the chunk).
//   - options: variadic functional options to configure the processing behavior.
//     CsvProcessingConfig contains Optional configurations including:
//   - BreakOnError: if set to true, processing stops on encountering an error; default is true.
//   - HeaderPresent: if set to true, assumes the CSV file contains a header row; default is true.
//   - ProcessOnce: if set to true, processing stops after one chunk processed; default is false.
//     Use SetHeaderPresent, SetProcessOnce and SetBreakOnError as agruments to set these options
//
// The function returns a slice of structs, each containing the result of applying the function on a chunk and any associated error.
// Each struct has two fields: Data (containing the result of the function) and Err (containing any encountered error).
func (fia *FileImportAuditUtility) ProcessCSVDataWithFunction(bucket string, inputURL string, fileByteSize int64,
	dataProcessorFunction func(CsvProcessingWithFunctionInput) (interface{}, error), options ...func(c *CsvProcessingConfig)) []CsvProcessingWithFunctionResult {

	optionsObj := &CsvProcessingConfig{
		BreakOnError:  true,
		HeaderPresent: true,
		ProcessOnce:   false,
	}
	for _, opt := range options {
		opt(optionsObj)
	}

	s3BaseUrl, _, _, _ := getPathComponents(bucket, inputURL)
	filePath := strings.TrimPrefix(inputURL, s3BaseUrl)
	fileLength, _, _ := fia.S3.GetFileLengthAndContentType(bucket, filePath, fia.Logger.GetLogrusEntry())
	var currentOffset int64 = 0
	var partialRowData string
	var header []string
	var err error
	index := 0
	var resultArr []CsvProcessingWithFunctionResult
	for currentOffset < fileLength {
		var base64encoding string
		base64encoding, currentOffset, err = fia.S3.FileDownloadByRange(bucket, filePath, fia.Logger.GetLogrusEntry(), currentOffset, fileByteSize, fileLength)
		if err != nil {
			resultArr = append(resultArr, CsvProcessingWithFunctionResult{Data: nil, Err: err})
			if optionsObj.BreakOnError {
				return resultArr
			}
		}
		csvDataBytes, err := base64.StdEncoding.DecodeString(base64encoding)
		if err != nil {
			return resultArr
		}
		var csvDataArray [][]string
		csvDataArray, partialRowData, err = readAllCSVWithPartialData(partialRowData+string(csvDataBytes), currentOffset >= fileLength)
		if err != nil {
			resultArr = append(resultArr, CsvProcessingWithFunctionResult{Err: err})
			if optionsObj.BreakOnError {
				return resultArr
			}
		}
		if len(csvDataArray) == 0 {
			continue
		}
		if optionsObj.HeaderPresent && header == nil {
			header = csvDataArray[0]
			csvDataArray = csvDataArray[1:]
		}
		if len(csvDataArray) > 0 {
			if optionsObj.HeaderPresent {
				csvDataArray = append([][]string{header}, csvDataArray...)
			}
			inputData := CsvProcessingWithFunctionInput{
				Data: csvDataArray,
				MetaData: CsvProcessingWithFunctionInputMetaData{
					Offset: currentOffset - int64(len(partialRowData)),
					Index:  index,
				},
			}
			result, err := dataProcessorFunction(inputData)
			if err != nil {
				resultArr = append(resultArr, CsvProcessingWithFunctionResult{Err: err})
				if optionsObj.BreakOnError {
					return resultArr
				}
			}
			resultArr = append(resultArr, CsvProcessingWithFunctionResult{Data: result, Err: nil})
			if optionsObj.ProcessOnce {
				return resultArr
			}
			index++
		}
	}
	return resultArr
}

func (fia *FileImportAuditUtility) ValidateFileImportData(bucket string, inputURL string, validations ...func(*validationHelper) error) error {
	obj := NewValidationHelper(fia, bucket, inputURL)
	for _, fun := range validations {
		err := fun(obj)
		if err != nil {
			return err
		}
	}
	return nil
}

func (fia *FileImportAuditUtility) GetFileData(bucket string, sheetName string, auditEntryId int) (fileData [][]string, url string, err error) {
	fileImportAudit, err := fia.UpdateAuditEntry(auditEntryId, map[string]interface{}{"status": FileStatusProgress})
	if err != nil {
		return [][]string{}, "", err
	}
	fileData, err = fia.downloadFileAndGetRows(bucket, sheetName, fileImportAudit.InputUrl)
	return fileData, fileImportAudit.InputUrl, err
}

// GetCSVFileData retrieves CSV file data from a specified S3 bucket and audit entry ID. It first updates the audit entry status to 'progress',
// then validates the file's content type to ensure it's a CSV. After downloading and decoding the base64-encoded file from S3,
// it reads and returns the CSV data as a slice of string slices. If any step fails, the function returns an error detailing the issue encountered.
func (fia *FileImportAuditUtility) GetCSVFileData(bucket string, auditEntryId int) (result [][]string, url string, err error) {
	fileImportAudit, err := fia.UpdateAuditEntry(auditEntryId, map[string]interface{}{"status": FileStatusProgress})
	if err != nil {
		return [][]string{}, "", err
	}
	s3FileUrl := fileImportAudit.InputUrl
	folderPath := strings.TrimPrefix(s3FileUrl, s3Protocol+bucket+s3RegionArn)

	_, contentType, err := fia.S3.GetFileLengthAndContentType(bucket, folderPath, fia.Logger.GetLogrusEntry())
	if contentType != CsvContentType {
		err = errors.New("file type is not CSV")
		return [][]string{}, "", err
	}
	base64encoding, _, err := fia.S3.FileDownload(bucket, folderPath, fia.Logger.GetLogrusEntry())
	if err != nil {
		fia.Logger.WithField("err", err).Error("error downloading s3 file")
		return [][]string{}, s3FileUrl, err
	}
	fileBytes, err := base64.StdEncoding.DecodeString(base64encoding)
	if err != nil {
		fia.Logger.WithField("err", err).Error("Could not convert base64 to bytes")
		return [][]string{}, s3FileUrl, err
	}
	r := csv.NewReader(bytes.NewReader(fileBytes))
	data, err := readDataFromCsvReader(r)
	if err != nil {
		fia.Logger.WithField("err", err).Error("Could not read CSV data from the file")
		return [][]string{}, s3FileUrl, err
	}
	return data, s3FileUrl, nil
}
func (fia *FileImportAuditUtility) UploadOutputFileData(bucket string, rows [][]string, sheetName string, outputFilePath string, auditEntryId int) error {
	baseUrl := s3Protocol + bucket + s3RegionArn
	buff, err := writeToExcel([][][]string{rows}, []string{sheetName}, fia.Logger)
	if err != nil {
		fia.Logger.WithField("err", err).Error("Error while writing to excel")
		return err
	}
	fileStatus := getFileStatus(rows)
	err = fia.uploadOutputAndUpdateAuditEntry(bucket, fileStatus, auditEntryId, outputFilePath, buff, baseUrl)
	if err != nil {
		fia.Logger.WithField("err", err).Error("Error while uploading output to s3")
		return err
	}
	err = fia.handleBatchCompleteEvent(auditEntryId, bucket, sheetName)
	if err != nil {
		fia.Logger.WithField("err", err).Error("error in batch complete event handling")
		return err
	}
	return nil
}

func (fia *FileImportAuditUtility) UploadInputFileDataMultiSheet(bucket string, filePath string, fileData []byte, sheetNames []string, tag string, userId int64,
	kafkaMessageConfig KafkaMessageConfig,
	schemaValidationMap map[string]func(fileData [][]string) (bool, error)) (string, error) {
	fia.setKafkaProducer()
	s3BaseUrl, fileName, _, extension := getPathComponents(bucket, filePath)
	_, err := fia.S3.Fileupload(bucket, base64.StdEncoding.EncodeToString(fileData), filePath, extension[1:], fia.Logger.GetLogrusEntry())
	if err != nil {
		fia.Logger.WithField("err", err).Error("Error uploading file to s3 multi sheet")
		return "Failed", err
	}
	for _, sheetName := range sheetNames {
		excelRows, err := readFromExcel(sheetName, fileData, fia.Logger)
		if err != nil {
			fia.Logger.WithField("err", err).Error("Error in excel processing multi sheet")
			return "Failed", err
		}
		if _, ok := schemaValidationMap[sheetName]; !ok {
			return "Failed", errors.New("validation not found for sheet " + sheetName)
		}
		isSchemaValid, err := schemaValidationMap[sheetName](excelRows)
		if !isSchemaValid || err != nil {
			err := fia.addSchemaFailedAuditEntry(s3BaseUrl, filePath, fileName+extension, tag, userId, err)
			if err != nil {
				return "Failed", err
			}
			return "Failed", errors.New("excel file has invalid schema for multi sheet")
		}
	}
	outputUrls := []string{s3BaseUrl + filePath}
	err = fia.addAuditEntryAndSendToKafka(outputUrls, kafkaMessageConfig, tag, userId)
	if err != nil {
		fia.Logger.WithField("err", err).Error("Error while completing transaction for audit entry and kafka send event")
		return "Failed", err
	}
	return "Success", nil
}

func (fia *FileImportAuditUtility) GetFileDataMultiSheet(bucket string, sheetNames []string, auditEntryId int) (res [][][]string, url string, err error) {
	fileImportAudit, err := fia.UpdateAuditEntry(auditEntryId, map[string]interface{}{"status": FileStatusProgress})
	if err != nil {
		return [][][]string{}, "", err
	}
	for _, sheetName := range sheetNames {
		fileData, err := fia.downloadFileAndGetRows(bucket, sheetName, fileImportAudit.InputUrl)
		if err != nil {
			return [][][]string{}, "", err
		}
		res = append(res, fileData)
	}
	return res, fileImportAudit.InputUrl, err
}

func (fia *FileImportAuditUtility) UploadOutputFileDataMultiSheet(bucket string, rows [][][]string, sheetNames []string, outputFilePath string, auditEntryId int, sheetNameForFileStatus string) error {
	if len(rows) != len(sheetNames) {
		return errors.New("length of sheet and length of data differ")
	}
	baseUrl := s3Protocol + bucket + s3RegionArn
	var fileStatus string
	for index, sheet := range sheetNames {
		if sheet == sheetNameForFileStatus {
			fileStatus = getFileStatus(rows[index])
		}
	}
	buff, err := writeToExcel(rows, sheetNames, fia.Logger)
	if err != nil {
		fia.Logger.WithField("err", err).Error("Error while writing to excel multi sheet")
		return err
	}
	err = fia.uploadOutputAndUpdateAuditEntry(bucket, fileStatus, auditEntryId, outputFilePath, buff, baseUrl)
	if err != nil {
		fia.Logger.WithField("err", err).Error("Error while uploading output to s3 multi sheet")
		return err
	}
	return nil
}
