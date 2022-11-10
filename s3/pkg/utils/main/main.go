package main

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"

	//"github.com/soda/multi-cloud/s3/pkg/utils"
	"io"
	"io/ioutil"
)

type Tag struct {
	key []byte
	iv  []byte
}

/*func main() {

	// Open up our database connection.
	// I've set up a database on my local machine using phpmyadmin.
	// The database is called testDb
	db, err := sql.Open("mysql", "root:@tcp(127.0.0.1:4000)/s3")

	// if there is an error opening the connection, handle it
	if err != nil {
		panic(err.Error())
	}

	// Execute the query
	results, err := db.Query("SELECT sseserverkey, sseiv FROM bucket_sseopts")
	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}

	var tag Tag
	for results.Next() {

		// for each row, scan the result into our tag composite object
		err = results.Scan(&tag.key, &tag.iv)
		if err != nil {
			panic(err.Error()) // proper error handling instead of panic in your app
		}
		// and then print out the tag's Name attribute
		fmt.Println(tag)
	}

	// defer the close till after the main function has finished
	// executing
	defer db.Close()

	// encrypt
	// read file to byte[]
	allBytes, readErr := ioutil.ReadFile("/home/prakash/Downloads/tigervnc64-1.8.0.exe")
	fmt.Println(readErr)

	allEncBytes := make([]byte, 0)

	thouBytes := make([]byte, 0)
	for i := 0; i < 5242880; i++ {

		thouBytes = append(thouBytes, allBytes[i])
	}
	fmt.Println("\t\t thoubytes.len",len(thouBytes))
	// encrypt in chunks of 1000, write file to disk
	var out bytes.Buffer
	block, err := aes.NewCipher(tag.key)
	if err != nil {
		panic(err)
	}
	stream := cipher.NewCTR(block, tag.iv)
	writer := &cipher.StreamWriter{S: stream, W: &out}

	// Copy the input to the output buffer, encrypting as we go.
	if _, err := io.Copy(writer, bytes.NewReader(thouBytes)); err != nil {
		panic(err)
	}

	allEncBytes = append(allEncBytes, out.Bytes()...)


	// block2
	thouBytes = make([]byte, 0)
	for i := 5242880; i < 5242880*2; i++ {

		thouBytes = append(thouBytes, allBytes[i])
	}
	fmt.Println("\t\t thoubytes.len",len(thouBytes))
	// encrypt in chunks of 1000, write file to disk
	var out1 bytes.Buffer

	if err != nil {
		panic(err)
	}
	stream1 := cipher.NewCTR(block, tag.iv)
	writer1 := &cipher.StreamWriter{S: stream1, W: &out1}

	// Copy the input to the output buffer, encrypting as we go.
	if _, err := io.Copy(writer1, bytes.NewReader(thouBytes)); err != nil {
		panic(err)
	}

	allEncBytes = append(allEncBytes, out1.Bytes()...)

	// block 3
	thouBytes = make([]byte, 0)
	for i := 5242880*2; i < (5242880*2)+791560; i++ {

		thouBytes = append(thouBytes, allBytes[i])
	}
	fmt.Println("\t\t thoubytes.len",len(thouBytes))
	// encrypt in chunks of 1000, write file to disk
	var out2 bytes.Buffer

	if err != nil {
		panic(err)
	}
	stream2 := cipher.NewCTR(block, tag.iv)
	writer2 := &cipher.StreamWriter{S: stream2, W: &out2}

	// Copy the input to the output buffer, encrypting as we go.
	if _, err := io.Copy(writer2, bytes.NewReader(thouBytes)); err != nil {
		panic(err)
	}

	allEncBytes = append(allEncBytes, out2.Bytes()...)


	// write file to disk
	writeErr := ioutil.WriteFile("/home/prakash/Downloads/tigervnc64-1.8.0mainenc.exe", allEncBytes, 0777)
	if writeErr != nil {
		panic(writeErr)
	}

	// decrypt
	fullBytes1, _ := ioutil.ReadFile("/home/prakash/Downloads/tigervnc64-1.8.0mainenc.exe")
	fmt.Println("\t\t fullBytes1.len", len(fullBytes1))
	fullBytes := make([]byte, 0)

	//block, err := aes.NewCipher(tag.key)
	if err != nil {
		panic(err)
	}

	//bcountmax := 11277320
	bchunk1 := 5242880
	bchunk2 := 5242880
	bchunk3 := 791560

	newBuf := make([]byte, 0)
	bindex := 0
	for bindex < bchunk1 {
		newBuf = append(newBuf, fullBytes1[bindex])
		bindex = bindex + 1
	}
	fmt.Println("\t\t bindex = ", bindex)
	fmt.Println("\t\t newBuf.len", len(newBuf))

	finalReader := &cipher.StreamReader{S: stream, R: bytes.NewReader(newBuf)}
	// Copy the input to the output stream, decrypting as we go.
	// now, read out the chunks, decrypting as we read
	nonEncBuf, _ := ioutil.ReadAll(finalReader)
	fullBytes = append(fullBytes, nonEncBuf...)
	fmt.Println("\t\t fullBytes.len", len(fullBytes))
	fmt.Println(bytes.Equal(newBuf, nonEncBuf))

	newBuf1 := make([]byte, 0)
	for bindex < (bchunk1 + bchunk2) {
		newBuf1 = append(newBuf1, fullBytes1[bindex])
		bindex = bindex + 1
	}
	fmt.Println("\t\t bindex = ", bindex)
	fmt.Println("\t\t newBuf1.len", len(newBuf1))
	finalReader1 := &cipher.StreamReader{S: stream, R: bytes.NewReader(newBuf1)}
	// Copy the input to the output stream, decrypting as we go.
	// now, read out the chunks, decrypting as we read
	nonEncBuf1, _ := ioutil.ReadAll(finalReader1)
	fullBytes = append(fullBytes, nonEncBuf1...)
	fmt.Println("\t\t fullBytes.len", len(fullBytes))
	fmt.Println(bytes.Equal(newBuf1, nonEncBuf1))

	newBuf2 := make([]byte, 0)
	for bindex < (bchunk1 + bchunk2 + bchunk3) {
		newBuf2 = append(newBuf2, fullBytes1[bindex])
		bindex = bindex + 1
	}
	fmt.Println("\t\t bindex = ", bindex)
	fmt.Println("\t\t newBuf2.len", len(newBuf2))
	finalReader2 := &cipher.StreamReader{S: stream, R: bytes.NewReader(newBuf2)}
	// Copy the input to the output stream, decrypting as we go.
	// now, read out the chunks, decrypting as we read
	nonEncBuf2, _ := ioutil.ReadAll(finalReader2)
	fullBytes = append(fullBytes, nonEncBuf2...)
	fmt.Println("\t\t fullBytes.len", len(fullBytes))
	fmt.Println(bytes.Equal(newBuf2, nonEncBuf2))

	//newBuf = make([]byte, 791560)

	ioutil.WriteFile("/home/prakash/Downloads/tigervnc64-1.8.0fromcloudramain.exe", fullBytes, 0777)

}*/

func main() {
	// Open up our database connection.
	// I've set up a database on my local machine using phpmyadmin.
	// The database is called testDb
	db, err := sql.Open("mysql", "root:@tcp(127.0.0.1:4000)/s3")

	// if there is an error opening the connection, handle it
	if err != nil {
		panic(err.Error())
	}

	// Execute the query
	results, err := db.Query("SELECT sseserverkey, sseiv FROM bucket_sseopts")
	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}

	var tag Tag
	for results.Next() {

		// for each row, scan the result into our tag composite object
		err = results.Scan(&tag.key, &tag.iv)
		if err != nil {
			panic(err.Error()) // proper error handling instead of panic in your app
		}
		// and then print out the tag's Name attribute
		fmt.Println(tag)
	}

	// defer the close till after the main function has finished
	// executing
	defer db.Close()
	//keyArr, keyErr := utils.GetRandomNBitKey(32)
	//if keyErr != nil {
	//	panic(keyErr)
	//}
	//ivArr, ivErr := utils.GetRandomNBitKey(16)
	//if ivErr != nil {
	//	panic(ivErr)
	//}
	keyArr := tag.key
	ivArr := tag.iv

	// read file to byte[]
	allBytes, readErr := ioutil.ReadFile("/home/prakash/Downloads/tigervnc64-1.8.0.exe")
	fmt.Println(readErr)

	allEncBytes := make([]byte, 0)
	allBytesLen := len(allBytes)
	modVal := allBytesLen / 5242880

	for i := 0; i < modVal; i++ {
		thouBytes := make([]byte, 0)
		for j := (i) * 5242880; j < (i+1)*5242880; j++ {
			thouBytes = append(thouBytes, allBytes[j])
		}
		// encrypt in chunks of 1000, write file to disk
		var out bytes.Buffer
		block, err := aes.NewCipher(keyArr)
		if err != nil {
			panic(err)
		}
		stream := cipher.NewCTR(block, ivArr)
		writer := &cipher.StreamWriter{S: stream, W: &out}

		// Copy the input to the output buffer, encrypting as we go.
		if _, err := io.Copy(writer, bytes.NewReader(thouBytes)); err != nil {
			panic(err)
		}

		allEncBytes = append(allEncBytes, out.Bytes()...)
	}
	{
		thouBytes := make([]byte, 0)
		for j := (modVal) * 5242880; j < allBytesLen; j++ {
			thouBytes = append(thouBytes, allBytes[j])
		}
		// encrypt in chunks of 1000, write file to disk
		var out bytes.Buffer
		block, err := aes.NewCipher(keyArr)
		if err != nil {
			panic(err)
		}
		stream := cipher.NewCTR(block, ivArr)
		writer := &cipher.StreamWriter{S: stream, W: &out}

		// Copy the input to the output buffer, encrypting as we go.
		if _, err := io.Copy(writer, bytes.NewReader(thouBytes)); err != nil {
			panic(err)
		}

		allEncBytes = append(allEncBytes, out.Bytes()...)
	}

	// write file to disk
	writeErr := ioutil.WriteFile("/home/prakash/Downloads/tigervnc64-1.8.0enc.exe", allEncBytes, 0777)
	if writeErr != nil {
		panic(writeErr)
	}

	// read encrypted file, decrypt and write back to disk

	// read file to byte[]
	readEncBytes, readErr := ioutil.ReadFile("/home/prakash/Downloads/tigervnc64-1.8.0fc.exe")
	fmt.Println(readErr)

	// decrypt, write file to disk

	allDecBytes := make([]byte, 0)

	//for i:=0;i<modVal;i++{
	//thouBytes := make([]byte, 0)
	//for j:=(i)*1000000;j<(i+1)*1000000;j++{
	//	thouBytes = append(thouBytes, allBytes[j])
	//}
	for i := 0; i < modVal; i++ {
		decThouBytes := make([]byte, 0)
		for j := (i) * 5242880; j < (i+1)*5242880; j++ {
			decThouBytes = append(decThouBytes, readEncBytes[j])
		}

		// encrypt in chunks of 1000, write file to disk
		var out bytes.Buffer
		block, err := aes.NewCipher(keyArr)
		if err != nil {
			panic(err)
		}
		stream := cipher.NewCTR(block, ivArr)
		writer := &cipher.StreamWriter{S: stream, W: &out}

		// Copy the input to the output buffer, encrypting as we go.
		if _, err := io.Copy(writer, bytes.NewReader(decThouBytes)); err != nil {
			panic(err)
		}

		allDecBytes = append(allDecBytes, out.Bytes()...)

	}

	{
		decThouBytes := make([]byte, 0)
		for j := (modVal) * 5242880; j < allBytesLen; j++ {
			decThouBytes = append(decThouBytes, readEncBytes[j])
		}

		// encrypt in chunks of 1000, write file to disk
		var out bytes.Buffer
		block, err := aes.NewCipher(keyArr)
		if err != nil {
			panic(err)
		}
		stream := cipher.NewCTR(block, ivArr)
		writer := &cipher.StreamWriter{S: stream, W: &out}

		// Copy the input to the output buffer, encrypting as we go.
		if _, err := io.Copy(writer, bytes.NewReader(decThouBytes)); err != nil {
			panic(err)
		}

		allDecBytes = append(allDecBytes, out.Bytes()...)

	}

	// write file to disk
	writeErr = ioutil.WriteFile("/home/prakash/Downloads/tigervnc64-1.8.0dec.exe", allDecBytes, 0777)
	if writeErr != nil {
		panic(writeErr)
	}

	fmt.Println("\n\n\t\t", bytes.Equal(allBytes, allDecBytes))

}

/*func main() {
	keyArr, keyErr := utils.GetRandomNBitKey(32)
	if keyErr != nil {
		panic(keyErr)
	}
	ivArr, ivErr := utils.GetRandomNBitKey(16)
	if ivErr != nil {
		panic(ivErr)
	}

	// read file to byte[]
	allBytes, readErr := ioutil.ReadFile("/home/prakash/Downloads/nppinstaller.exe")
	fmt.Println(readErr)

	// encrypt, write file to disk
	var out bytes.Buffer
	block, err := aes.NewCipher(keyArr)
	if err != nil {
		panic(err)
	}
	stream := cipher.NewOFB(block, ivArr)
	writer := &cipher.StreamWriter{S: stream, W: &out}

	// Copy the input to the output buffer, encrypting as we go.
	// Copy the input to the output buffer, encrypting as we go.
	if _, err := io.Copy(writer, bytes.NewReader(allBytes)); err != nil {
		panic(err)
	}

	// write file to disk
	writeErr := ioutil.WriteFile("/home/prakash/Downloads/nppinstallerenc.exe", out.Bytes(), 0777)
	if writeErr != nil {
		panic(writeErr)
	}

	// read encrypted file, decrypt and write back to disk

	// read file to byte[]
	allEncBytes, readErr := ioutil.ReadFile("/home/prakash/Downloads/nppinstallerenc.exe")
	fmt.Println(readErr)

	// decrypt, write file to disk
	block, err = aes.NewCipher(keyArr)
	if err != nil {
		panic(err)
	}
	stream = cipher.NewOFB(block, ivArr)
	reader := &cipher.StreamReader{S: stream, R: bytes.NewReader(allEncBytes)}

	// Copy the input to the output buffer, decrypting as we go.
	decBytes, decErr := ioutil.ReadAll(reader)
	if decErr != nil {
		panic(decErr)
	}
	// write file to disk
	writeErr = ioutil.WriteFile("/home/prakash/Downloads/nppinstallerdec.exe", decBytes, 0777)
	if writeErr != nil {
		panic(writeErr)
	}

	fmt.Println("\n\n\t\t", bytes.Equal(allBytes, decBytes))

}*/
