package hedera

type Status uint32

const (
	StatusOk                                  Status = 0
	StatusInvalidTransaction                  Status = 1
	StatusPayerAccountNotFound                Status = 2
	StatusInvalidNodeAccount                  Status = 3
	StatusTransactionExpired                  Status = 4
	StatusInvalidTransactionStart             Status = 5
	StatusInvalidTransactionDuration          Status = 6
	StatusInvalidSignature                    Status = 7
	StatusMemoTooLong                         Status = 8
	StatusInsufficientTxFee                   Status = 9
	StatusInsufficientPayerBalance            Status = 10
	StatusDuplicateTransaction                Status = 11
	StatusBusy                                Status = 12
	StatusNotSupported                        Status = 13
	StatusInvalidFileID                       Status = 14
	StatusInvalidAccountID                    Status = 15
	StatusInvalidContractID                   Status = 16
	StatusInvalidTransactionID                Status = 17
	StatusReceiptNotFound                     Status = 18
	StatusRecordNotFound                      Status = 19
	StatusInvalidSolidityID                   Status = 20
	StatusUnknown                             Status = 21
	StatusSuccess                             Status = 22
	StatusFailInvalid                         Status = 23
	StatusFailFee                             Status = 24
	StatusFailBalance                         Status = 25
	StatusKeyRequired                         Status = 26
	StatusBadEncoding                         Status = 27
	StatusInsufficientAccountBalance          Status = 28
	StatusInvalidSolidityAddress              Status = 29
	StatusInsufficientGas                     Status = 30
	StatusContractSizeLimitExceeded           Status = 31
	StatusLocalCallModificationException      Status = 32
	StatusContractRevertExecuted              Status = 33
	StatusContractExecutionException          Status = 34
	StatusInvalidReceivingNodeAccount         Status = 35
	StatusMissingQueryHeader                  Status = 36
	StatusAccountUpdateFailed                 Status = 37
	StatusInvalidKeyEncoding                  Status = 38
	StatusNullSolidityAddress                 Status = 39
	StatusContractUpdateFailed                Status = 40
	StatusInvalidQueryHeader                  Status = 41
	StatusInvalidFeeSubmitted                 Status = 42
	StatusInvalidPayerSignature               Status = 43
	StatusKeyNotProvided                      Status = 44
	StatusInvalidExpirationTime               Status = 45
	StatusNoWaclKey                           Status = 46
	StatusFileContentEmpty                    Status = 47
	StatusInvalidAccountAmounts               Status = 48
	StatusEmptyTransactionBody                Status = 49
	StatusInvalidTransactionBody              Status = 50
	StatusInvalidSignatureTypeMismatchingKey  Status = 51
	StatusInvalidSignatureCountMismatchingKey Status = 52
	StatusEmptyClaimBody                      Status = 53
	StatusEmptyClaimHash                      Status = 54
	StatusEmptyClaimKeys                      Status = 55
	StatusInvalidClaimHashSize                Status = 56
	StatusEmptyQueryBody                      Status = 57
	StatusEmptyClaimQuery                     Status = 58
	StatusClaimNotFound                       Status = 59
	StatusAccountIDDoesNotExist               Status = 60
	StatusClaimAlreadyExists                  Status = 61
	StatusInvalidFileWacl                     Status = 62
	StatusSerializationFailed                 Status = 63
	StatusTransactionOversize                 Status = 64
	StatusTransactionTooManyLayers            Status = 65
	StatusContractDeleted                     Status = 66
	StatusPlatformNotActive                   Status = 67
	StatusKeyPrefixMismatch                   Status = 68
	StatusPlatformTransactionNotCreated       Status = 69
	StatusInvalidRenewalPeriod                Status = 70
	StatusInvalidPayerAccountID               Status = 71
	StatusAccountDeleted                      Status = 72
	StatusFileDeleted                         Status = 73
	StatusAccountRepeatedInAccountAmounts     Status = 74
	StatusSettingNegativeAccountBalance       Status = 75
	StatusObtainerRequired                    Status = 76
	StatusObtainerSameContractID              Status = 77
	StatusObtainerDoesNotExist                Status = 78
	StatusModifyingImmutableContract          Status = 79
	StatusFileSystemException                 Status = 80
	StatusAutorenewDurationNotInRange         Status = 81
	StatusErrorDecodingBytestring             Status = 82
	StatusContractFileEmpty                   Status = 83
	StatusContractBytecodeEmpty               Status = 84
	StatusInvalidInitialBalance               Status = 85
	StatusInvalidReceiveRecordThreshold       Status = 86
	StatusInvalidSendRecordThreshold          Status = 87
	StatusAccountIsNotGenesisAccount          Status = 88
	StatusPayerAccountUnauthorized            Status = 89
	StatusInvalidFreezeTransactionBody        Status = 90
	StatusFreezeTransactionBodyNotFound       Status = 91
	StatusTransferListSizeLimitExceeded       Status = 92
	StatusResultSizeLimitExceeded             Status = 93
	StatusNotSpecialAccount                   Status = 94
	StatusContractNegativeGas                 Status = 95
	StatusContractNegativeValue               Status = 96
	StatusInvalidFeeFile                      Status = 97
	StatusInvalidExchangeRateFile             Status = 98
	StatusInsufficientLocalCallGas            Status = 99
	StatusEntityNotAllowedToDelete            Status = 100
	StatusAuthorizationFailed                 Status = 101
	StatusFileUploadedProtoInvalid            Status = 102
	StatusFileUploadedProtoNotSavedToDisk     Status = 103
	StatusFeeScheduleFilePartUploaded         Status = 104
	StatusExchangeRateChangeLimitExceeded     Status = 105
	StatusMaxContractStorageExceeded          Status = 106
	StatusTransferAccountSameAsDeleteAccount  Status = 107
	StatusTotalLedgerBalanceInvalid           Status = 108
	StatusExpirationReductionNotAllowed       Status = 110
	StatusMaxGasLimitExceeded                 Status = 111
	StatusMaxFileSizeExceeded                 Status = 112
)

func (status Status) isExceptional(includeUnknown bool) bool {
	switch status {
	case StatusOk, StatusSuccess:
		return false
	case StatusUnknown, StatusReceiptNotFound, StatusRecordNotFound:
		if !includeUnknown {
			return false
		}
	default:
	}

	return true
}

func (status Status) String() string {
	switch status {
	case StatusOk:
		return "OK"
	case StatusInvalidTransaction:
		return "INVALID_TRANSACTION"
	case StatusPayerAccountNotFound:
		return "PAYER_ACCOUNT_NOT_FOUND"
	case StatusInvalidNodeAccount:
		return "INVALID_NODE_ACCOUNT"
	case StatusTransactionExpired:
		return "TRANSACTION_EXPIRED"
	case StatusInvalidTransactionStart:
		return "INVALID_TRANSACTION_START"
	case StatusInvalidTransactionDuration:
		return "INVALID_TRANSACTION_DURATION"
	case StatusInvalidSignature:
		return "INVALID_SIGNATURE"
	case StatusMemoTooLong:
		return "MEMO_TOO_LONG"
	case StatusInsufficientTxFee:
		return "INSUFFICIENT_TX_FEE"
	case StatusInsufficientPayerBalance:
		return "INSUFFICIENT_PAYER_BALANCE"
	case StatusDuplicateTransaction:
		return "DUPLICATE_TRANSACTION"
	case StatusBusy:
		return "BUSY"
	case StatusNotSupported:
		return "NOT_SUPPORTED"
	case StatusInvalidFileID:
		return "INVALID_FILE_ID"
	case StatusInvalidAccountID:
		return "INVALID_ACCOUNT_ID"
	case StatusInvalidContractID:
		return "INVALID_CONTRACT_ID"
	case StatusInvalidTransactionID:
		return "INVALID_TRANSACTION_ID"
	case StatusReceiptNotFound:
		return "RECEIPT_NOT_FOUND"
	case StatusRecordNotFound:
		return "RECORD_NOT_FOUND"
	case StatusInvalidSolidityID:
		return "INVALID_SOLIDITY_ID"
	case StatusUnknown:
		return "UNKNOWN"
	case StatusSuccess:
		return "SUCCESS"
	case StatusFailInvalid:
		return "FAIL_INVALID"
	case StatusFailFee:
		return "FAIL_FEE"
	case StatusFailBalance:
		return "FAIL_BALANCE"
	case StatusKeyRequired:
		return "KEY_REQUIRED"
	case StatusBadEncoding:
		return "BAD_ENCODING"
	case StatusInsufficientAccountBalance:
		return "INSUFFICIENT_ACCOUNT_BALANCE"
	case StatusInvalidSolidityAddress:
		return "INVALID_SOLIDITY_ADDRESS"
	case StatusInsufficientGas:
		return "INSUFFICIENT_GAS"
	case StatusContractSizeLimitExceeded:
		return "CONTRACT_SIZE_LIMIT_EXCEEDED"
	case StatusLocalCallModificationException:
		return "LOCAL_CALL_MODIFICATION_EXCEPTION"
	case StatusContractRevertExecuted:
		return "CONTRACT_REVERT_EXECUTED"
	case StatusContractExecutionException:
		return "CONTRACT_EXECUTION_EXCEPTION"
	case StatusInvalidReceivingNodeAccount:
		return "INVALID_RECEIVING_NODE_ACCOUNT"
	case StatusMissingQueryHeader:
		return "MISSING_QUERY_HEADER"
	case StatusAccountUpdateFailed:
		return "ACCOUNT_UPDATE_FAILED"
	case StatusInvalidKeyEncoding:
		return "INVALID_KEY_ENCODING"
	case StatusNullSolidityAddress:
		return "NULL_SOLIDITY_ADDRESS"
	case StatusContractUpdateFailed:
		return "CONTRACT_UPDATE_FAILED"
	case StatusInvalidQueryHeader:
		return "INVALID_QUERY_HEADER"
	case StatusInvalidFeeSubmitted:
		return "INVALID_FEE_SUBMITTED"
	case StatusInvalidPayerSignature:
		return "INVALID_PAYER_SIGNATURE"
	case StatusKeyNotProvided:
		return "KEY_NOT_PROVIDED"
	case StatusInvalidExpirationTime:
		return "INVALID_EXPIRATION_TIME"
	case StatusNoWaclKey:
		return "NO_WACL_KEY"
	case StatusFileContentEmpty:
		return "FILE_CONTENT_EMPTY"
	case StatusInvalidAccountAmounts:
		return "INVALID_ACCOUNT_AMOUNTS"
	case StatusEmptyTransactionBody:
		return "EMPTY_TRANSACTION_BODY"
	case StatusInvalidTransactionBody:
		return "INVALID_TRANSACTION_BODY"
	case StatusInvalidSignatureTypeMismatchingKey:
		return "INVALID_SIGNATURE_TYPE_MISMATCHING_KEY"
	case StatusInvalidSignatureCountMismatchingKey:
		return "INVALID_SIGNATURE_COUNT_MISMATCHING_KEY"
	case StatusEmptyClaimBody:
		return "EMPTY_CLAIM_BODY"
	case StatusEmptyClaimHash:
		return "EMPTY_CLAIM_HASH"
	case StatusEmptyClaimKeys:
		return "EMPTY_CLAIM_KEYS"
	case StatusInvalidClaimHashSize:
		return "INVALID_CLAIM_HASH_SIZE"
	case StatusEmptyQueryBody:
		return "EMPTY_QUERY_BODY"
	case StatusEmptyClaimQuery:
		return "EMPTY_CLAIM_QUERY"
	case StatusClaimNotFound:
		return "CLAIM_NOT_FOUND"
	case StatusAccountIDDoesNotExist:
		return "ACCOUNT_ID_DOES_NOT_EXIST"
	case StatusClaimAlreadyExists:
		return "CLAIM_ALREADY_EXISTS"
	case StatusInvalidFileWacl:
		return "INVALID_FILE_WACL"
	case StatusSerializationFailed:
		return "SERIALIZATION_FAILED"
	case StatusTransactionOversize:
		return "TRANSACTION_OVERSIZE"
	case StatusTransactionTooManyLayers:
		return "TRANSACTION_TOO_MANY_LAYERS"
	case StatusContractDeleted:
		return "CONTRACT_DELETED"
	case StatusPlatformNotActive:
		return "PLATFORM_NOT_ACTIVE"
	case StatusKeyPrefixMismatch:
		return "KEY_PREFIX_MISMATCH"
	case StatusPlatformTransactionNotCreated:
		return "PLATFORM_TRANSACTION_NOT_CREATED"
	case StatusInvalidRenewalPeriod:
		return "INVALID_RENEWAL_PERIOD"
	case StatusInvalidPayerAccountID:
		return "INVALID_PAYER_ACCOUNT_ID"
	case StatusAccountDeleted:
		return "ACCOUNT_DELETED"
	case StatusFileDeleted:
		return "FILE_DELETED"
	case StatusAccountRepeatedInAccountAmounts:
		return "ACCOUNT_REPEATED_IN_ACCOUNT_AMOUNTS"
	case StatusSettingNegativeAccountBalance:
		return "SETTING_NEGATIVE_ACCOUNT_BALANCE"
	case StatusObtainerRequired:
		return "OBTAINER_REQUIRED"
	case StatusObtainerSameContractID:
		return "OBTAINER_SAME_CONTRACT_ID"
	case StatusObtainerDoesNotExist:
		return "OBTAINER_DOES_NOT_EXIST"
	case StatusModifyingImmutableContract:
		return "MODIFYING_IMMUTABLE_CONTRACT"
	case StatusFileSystemException:
		return "FILE_SYSTEM_EXCEPTION"
	case StatusAutorenewDurationNotInRange:
		return "AUTORENEW_DURATION_NOT_IN_RANGE"
	case StatusErrorDecodingBytestring:
		return "ERROR_DECODING_BYTESTRING"
	case StatusContractFileEmpty:
		return "CONTRACT_FILE_EMPTY"
	case StatusContractBytecodeEmpty:
		return "CONTRACT_BYTECODE_EMPTY"
	case StatusInvalidInitialBalance:
		return "INVALID_INITIAL_BALANCE"
	case StatusInvalidReceiveRecordThreshold:
		return "INVALID_RECEIVE_RECORD_THRESHOLD"
	case StatusInvalidSendRecordThreshold:
		return "INVALID_SEND_RECORD_THRESHOLD"
	case StatusAccountIsNotGenesisAccount:
		return "ACCOUNT_IS_NOT_GENESIS_ACCOUNT"
	case StatusPayerAccountUnauthorized:
		return "PAYER_ACCOUNT_UNAUTHORIZED"
	case StatusInvalidFreezeTransactionBody:
		return "INVALID_FREEZE_TRANSACTION_BODY"
	case StatusFreezeTransactionBodyNotFound:
		return "FREEZE_TRANSACTION_BODY_NOT_FOUND"
	case StatusTransferListSizeLimitExceeded:
		return "TRANSFER_LIST_SIZE_LIMIT_EXCEEDED"
	case StatusResultSizeLimitExceeded:
		return "RESULT_SIZE_LIMIT_EXCEEDED"
	case StatusNotSpecialAccount:
		return "NOT_SPECIAL_ACCOUNT"
	case StatusContractNegativeGas:
		return "CONTRACT_NEGATIVE_GAS"
	case StatusContractNegativeValue:
		return "CONTRACT_NEGATIVE_VALUE"
	case StatusInvalidFeeFile:
		return "INVALID_FEE_FILE"
	case StatusInvalidExchangeRateFile:
		return "INVALID_EXCHANGE_RATE_FILE"
	case StatusInsufficientLocalCallGas:
		return "INSUFFICIENT_LOCAL_CALL_GAS"
	case StatusEntityNotAllowedToDelete:
		return "ENTITY_NOT_ALLOWED_TO_DELETE"
	case StatusAuthorizationFailed:
		return "AUTHORIZATION_FAILED"
	case StatusFileUploadedProtoInvalid:
		return "FILE_UPLOADED_PROTO_INVALID"
	case StatusFileUploadedProtoNotSavedToDisk:
		return "FILE_UPLOADED_PROTO_NOT_SAVED_TO_DISK"
	case StatusFeeScheduleFilePartUploaded:
		return "FEE_SCHEDULE_FILE_PART_UPLOADED"
	case StatusExchangeRateChangeLimitExceeded:
		return "EXCHANGE_RATE_CHANGE_LIMIT_EXCEEDED"
	case StatusMaxContractStorageExceeded:
		return "MAX_CONTRACT_STORAGE_EXCEEDED"
	case StatusTransferAccountSameAsDeleteAccount:
		return "TRANSAFER_ACCOUNT_SAME_AS_DELETE_ACCOUNT"
	case StatusTotalLedgerBalanceInvalid:
		return "TOTAL_LEDGER_BALANCE_INVALID"
	case StatusExpirationReductionNotAllowed:
		return "EXPIRATION_REDUCTION_NOT_ALLOWED"
	case StatusMaxGasLimitExceeded:
		return "MAX_GAS_LIMIT_EXCEEDED"
	case StatusMaxFileSizeExceeded:
		return "MAX_FILE_SIZE_EXCEEDED"
	}

	panic("unreacahble: Status.String() switch statement is non-exhaustive")
}
