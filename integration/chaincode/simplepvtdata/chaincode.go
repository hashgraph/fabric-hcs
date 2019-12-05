/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package simplepvtdata

import (
	"fmt"

	"github.com/hyperledger/fabric/core/chaincode/shim"
	pb "github.com/hyperledger/fabric/protos/peer"
)

// SimplePrivateDataCC example Chaincode implementation
type SimplePrivateDataCC struct {
}

// Init initializes chaincode
// ===========================
func (t *SimplePrivateDataCC) Init(stub shim.ChaincodeStubInterface) pb.Response {
	return shim.Success(nil)
}

// Invoke - Our entry point for Invocations
// ========================================
func (t *SimplePrivateDataCC) Invoke(stub shim.ChaincodeStubInterface) pb.Response {
	function, args := stub.GetFunctionAndParameters()
	fmt.Println("invoke is running " + function)

	// Handle different functions
	switch function {
	case "put":
		err := stub.PutPrivateData("~local", args[0], []byte(args[1]))
		if err != nil {
			return shim.Error(err.Error())
		}

		return shim.Success([]byte{})

	case "get":
		data, err := stub.GetPrivateData("~local", args[0])
		if err != nil {
			return shim.Error(err.Error())
		}

		return shim.Success(data)
	default:
		//error
		fmt.Println("invoke did not find func: " + function)
		return shim.Error("Received unknown function invocation")
	}
}
