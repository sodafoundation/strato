package utils

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
)

func TestGetRandom32BitKey(t *testing.T) {

	randomCount := rand.Intn(100)
	sliceByteArray := make([][]byte,0)

	for i:=0;i<randomCount;i++{
		// get a new []byte, check none of the earlier []byte equal this
		newByteArr,_ := GetRandom32BitKey()
		for _,byteArr := range sliceByteArray{
			assert.False(t, bytes.Equal(byteArr, newByteArr))
		}
		// add the new []byte to the array
		sliceByteArray = append(sliceByteArray, newByteArr)
	}
}

func TestEncAndDec(t *testing.T) {

	key, _ := GetRandom32BitKey()
	// raw data
	rawString := "abcd"
	rawData := []byte(rawString)
	// encrypt
	_, encData := EncryptWithAES256RandomKey(rawData, key)
	// decrypt
	_, decData := DecryptWithAES256(encData, key)
	decString := string(decData)
	// check
	assert.Equal(t, rawData, decData)
	assert.Equal(t, rawString, decString)
}
