//  Copyright (c) 2014 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bleve

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"

	"github.com/blevesearch/bleve/v2/index/upsidedown"
	"github.com/blevesearch/bleve/v2/util"
	index "github.com/blevesearch/bleve_index_api"
)

const metaFilename = "index_meta.json"

type indexMeta struct {
	Storage    string                 `json:"storage"`
	IndexType  string                 `json:"index_type"`
	Config     map[string]interface{} `json:"config,omitempty"`
	fileWriter *util.FileWriter
	fileReader *util.FileReader
}

func newIndexMeta(indexType string, storage string, config map[string]interface{}, path string) (*indexMeta, error) {
	indexMetaPath := indexMetaPath(path)
	fileWriter, err := util.NewFileWriter([]byte(indexMetaPath))
	if err != nil {
		return nil, fmt.Errorf("failed to create file writer for index meta: %w", err)
	}
	fileReader, err := util.NewFileReader(fileWriter.Id(), []byte(indexMetaPath))
	if err != nil {
		return nil, fmt.Errorf("failed to create file reader for index meta: %w", err)
	}
	return &indexMeta{
		IndexType:  indexType,
		Storage:    storage,
		Config:     config,
		fileWriter: fileWriter,
		fileReader: fileReader,
	}, nil
}

func openIndexMeta(path string) (*indexMeta, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, ErrorIndexPathDoesNotExist
	}
	indexMetaPath := indexMetaPath(path)
	metaBytes, err := os.ReadFile(indexMetaPath)
	if err != nil {
		return nil, ErrorIndexMetaMissing
	}

	// check if indexMetaPath+_temp exists, if so, this means a writer update was in progress
	// and we should attempt to recover using the temp file
	if _, err := os.Stat(indexMetaPath + "_temp"); err == nil {
		tempBytes, err := os.ReadFile(indexMetaPath + "_temp")
		if err == nil {
			err = os.Rename(indexMetaPath+"_temp", indexMetaPath)
			if err != nil {
				return nil, err
			}
			metaBytes = tempBytes
		}
	}

	var im indexMeta
	fileReader := &util.FileReader{}
	err = util.UnmarshalJSON(metaBytes, &im)
	if err != nil {
		if len(metaBytes) < 4 {
			return nil, ErrorIndexMetaCorrupt
		}

		pos := len(metaBytes) - 4
		fileWriterIDLen := int(binary.BigEndian.Uint32(metaBytes[pos:]))
		pos -= fileWriterIDLen
		if pos < 0 {
			return nil, ErrorIndexMetaCorrupt
		}

		fileWriterID := metaBytes[pos : pos+fileWriterIDLen]
		fileReader, err = util.NewFileReader(string(fileWriterID), []byte(indexMetaPath))
		if err != nil {
			return nil, err
		}

		buf, err := fileReader.Process(metaBytes[0:pos])
		if err != nil {
			return nil, err
		}
		err = util.UnmarshalJSON(buf, &im)
		if err != nil {
			return nil, ErrorIndexMetaCorrupt
		}
	}
	im.fileReader = fileReader

	if im.IndexType == "" {
		im.IndexType = upsidedown.Name
	}
	return &im, nil
}

func (i *indexMeta) Save(path string) (err error) {
	indexMetaPath := indexMetaPath(path)
	// ensure any necessary parent directories exist
	err = os.MkdirAll(path, 0700)
	if err != nil {
		if os.IsExist(err) {
			return ErrorIndexPathExists
		}
		return err
	}
	metaBytes, err := util.MarshalJSON(i)
	if err != nil {
		return err
	}
	indexMetaFile, err := os.OpenFile(indexMetaPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		if os.IsExist(err) {
			return ErrorIndexPathExists
		}
		return err
	}
	defer func() {
		if ierr := indexMetaFile.Close(); err == nil && ierr != nil {
			err = ierr
		}
	}()

	metaBytes = i.fileWriter.Process(metaBytes)

	_, err = indexMetaFile.Write(metaBytes)
	if err != nil {
		return err
	}

	_, err = indexMetaFile.Write([]byte(i.fileWriter.Id()))
	if err != nil {
		return err
	}

	err = binary.Write(indexMetaFile, binary.BigEndian, uint32(len(i.fileWriter.Id())))
	if err != nil {
		return err
	}

	return nil
}

func (i *indexMeta) CopyTo(path string, d index.Directory) (err error) {
	metaBytes, err := os.ReadFile(indexMetaPath(path))
	if err != nil {
		return err
	}

	w, err := d.GetWriter(metaFilename)
	if w == nil || err != nil {
		return fmt.Errorf("invalid writer for file: %s, err: %v",
			metaFilename, err)
	}
	defer w.Close()

	_, err = w.Write(metaBytes)
	return err
}

// updates the file callback writer id in the index meta,
// and re-processes data with the latest file callback writer
// returns the new file callback writer and reader to be used for
// future processing of index meta data
func (i *indexMeta) UpdateWriter(path string) error {
	indexMetaPath := indexMetaPath(path)
	metaBytes, err := util.MarshalJSON(i)
	if err != nil {
		return err
	}

	i.fileWriter, err = util.NewFileWriter([]byte(indexMetaPath))
	if err != nil {
		return err
	}

	metaBytes = i.fileWriter.Process(metaBytes)

	// write out new meta with new writer id, using temp file and rename to ensure atomicity
	// if we crash in the middle of this, on next open we will see the temp file and recover using it
	tempMetaPath := indexMetaPath + "_temp"
	tempMetaFile, err := os.OpenFile(tempMetaPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		if os.IsExist(err) {
			return ErrorIndexPathExists
		}
		return err
	}

	_, err = tempMetaFile.Write(metaBytes)
	if err != nil {
		return err
	}

	_, err = tempMetaFile.Write([]byte(i.fileWriter.Id()))
	if err != nil {
		return err
	}

	err = binary.Write(tempMetaFile, binary.BigEndian, uint32(len(i.fileWriter.Id())))
	if err != nil {
		return err
	}

	err = tempMetaFile.Close()
	if err != nil {
		return err
	}

	err = os.Rename(tempMetaPath, indexMetaPath)
	if err != nil {
		return err
	}

	i.fileReader, err = util.NewFileReader(string(i.fileWriter.Id()), []byte(indexMetaPath))
	if err != nil {
		return err
	}

	return nil
}

func indexMetaPath(path string) string {
	return filepath.Join(path, metaFilename)
}
