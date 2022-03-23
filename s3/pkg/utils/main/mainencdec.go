package main

import (
	"fmt"
	"io/ioutil"
	"math/rand"

	"github.com/soda/multi-cloud/s3/pkg/utils"
)

func main() {
	doEncAndDec()
}

func doEncAndDec() {
	content, err := ioutil.ReadFile("/root/smallimg.jpg")
	if err != nil {
		fmt.Println(err)
	}
	key, _ := GetRandom32BitKey()

	// encrypt
	_, encData := utils.EncryptWithAES256RandomKey(content, key)

	// write file to disk
	wencerr := ioutil.WriteFile("/root/smallimgenc.jpg", encData, 0777)
	if wencerr != nil {
		fmt.Println(wencerr)
	}

	fmt.Println("encrypted file written to disk, Press the Enter Key to decrypt after download from aws!")
	fmt.Scanln() // wait for Enter Key

	// decrypt
	encContent, rencerr := ioutil.ReadFile("/root/smallimgaws.jpg")
	if rencerr != nil {
		fmt.Println(rencerr)
	}
	_, decData := utils.DecryptWithAES256(encContent, key)

	werr := ioutil.WriteFile("/root/smallimgdec.jpg", decData, 0777)
	if werr != nil {
		fmt.Println(werr)
	}

}

func GetRandom32BitKey() ([]byte, error) {
	key := make([]byte, 32)

	_, err := rand.Read(key)
	if err != nil {
		fmt.Errorf("Error generating random 32 bit key %s", err)
		return nil, err
	}
	fmt.Println("Generated random 32 bit key")
	return key, nil
}
