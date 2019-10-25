package cli

import (
	"strings"

	"github.com/go-errors/errors"
	"github.com/spf13/cobra"

	"github.com/adrienkohlbecker/ejson-kms/utils"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	//"github.com/aws/aws-sdk-go/aws/client"
	"fmt"
	"os"
)

const docS3 = `
s3: Get/Put secrets file from aws s3.
`

const exampleS3 = `
ejson-kms s3get [bucket]/[key]
`

type downloader struct {
	*s3manager.Downloader
	bucket string
	file   string
	dir    string
}

func s3getCmd() *cobra.Command {

	cmd := &cobra.Command{
		Use:     "s3get",
		Short:   "get secrets from s3",
		Long:    strings.TrimSpace(docS3),
		Example: strings.TrimSpace(exampleS3),
	}

	var (
		storePath = ".secrets.json"
	)

	cmd.Flags().StringVar(&storePath, "path", storePath, "path of the secrets file")

	cmd.RunE = func(_ *cobra.Command, args []string) error {

		err := utils.ValidNewSecretsPath(storePath)
		if err != nil {
			return errors.WrapPrefix(err, "Invalid path", 0)
		}

		s3obj, err := utils.HasOneArgument(args)
		if err != nil {
			return errors.WrapPrefix(err, "Invalid s3 path", 0)
		}

		pos := strings.Index(s3obj, "/")
		if pos == -1 {
			return errors.WrapPrefix("not found '/'", "Invalid s3 path", 0)
		}

		s3o := strings.SplitN(s3obj, "/", 2)
		bucket := s3o[0]
		key := s3o[1]
		// err = store.Add(client, plaintext, name, description)
		// if err != nil {
		// 	return errors.WrapPrefix(err, "Unable to add secret", 0)
		// }

		// err = store.Save(storePath)
		// if err != nil {
		// 	return errors.WrapPrefix(err, "Unable to save JSON", 0)
		// }

		// cmd.Printf("Exported new secrets file at: %s\n", storePath)

		//S3に接続してダウンロード
		if err := Download(bucket, key, storePath); err != nil {
			fmt.Printf("Failed !!!%s", err)
		}

		return nil

	}

	return cmd

}

func Download(bucketName string, key string, filepath string) error {
	sess := session.Must(session.NewSession())
	client := s3.New(sess)

	params := &s3.ListObjectsInput{Bucket: &bucketName, Prefix: &key}

	//respにはkeyに部分一致したS3の全てのオブジェクトが入る
	resp, connectErr := client.ListObjects(params)
	if connectErr != nil {
		return connectErr
	}

	//respの中身にダウンロードするファイルがあるのかをチェック
	if !exists(key, resp) {
		return fmt.Errorf("S3 file not found")
	}

	manager := s3manager.NewDownloader(sess)
	d := downloader{bucket: bucketName, file: filepath, Downloader: manager}
	if file, err := d.downlowdFile(key); err != nil {
		if err := os.Remove(file); err != nil {
			fmt.Println(err)
		}
		return err
	}
	return nil
}

func exists(downloadFile string, resp *s3.ListObjectsOutput) bool {
	for _, content := range resp.Contents {
		if *content.Key == downloadFile {
			return true
		}
	}
	return false
}

func (d *downloader) downlowdFile(key string) (string, error) {

	//ダウンロード保存先に、ダウンロード用のファイルを作成
	file := d.file
	fs, err := os.Create(file)
	if err != nil {
		return file, err
	}
	defer fs.Close()

	fmt.Printf("download.. s3://%s/%s to %s...\n", d.bucket, key, file)
	params := &s3.GetObjectInput{Bucket: &d.bucket, Key: &key}

	//実際にダウンロードが行われるのはこのDownload関数
	if _, err := d.Download(fs, params); err != nil {
		return file, err
	}

	fmt.Printf("completed！ %s", file)

	return file, nil
}
