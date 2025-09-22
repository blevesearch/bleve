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
	Storage   string                 `json:"storage"`
	IndexType string                 `json:"index_type"`
	Config    map[string]interface{} `json:"config,omitempty"`
}

func newIndexMeta(indexType string, storage string, config map[string]interface{}) *indexMeta {
	return &indexMeta{
		IndexType: indexType,
		Storage:   storage,
		Config:    config,
	}
}

func openIndexMeta(path string) (*indexMeta, *util.FileReader, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, nil, ErrorIndexPathDoesNotExist
	}
	indexMetaPath := indexMetaPath(path)
	metaBytes, err := os.ReadFile(indexMetaPath)
	if err != nil {
		return nil, nil, ErrorIndexMetaMissing
	}

	var im indexMeta
	fileReader := &util.FileReader{}
	err = util.UnmarshalJSON(metaBytes, &im)
	if err != nil {
		if len(metaBytes) < 4 {
			return nil, nil, ErrorIndexMetaCorrupt
		}

		pos := len(metaBytes) - 4
		writerIdLen := int(binary.BigEndian.Uint32(metaBytes[pos:]))
		pos -= writerIdLen
		if pos < 0 {
			return nil, nil, ErrorIndexMetaCorrupt
		}

		writerId := metaBytes[pos : pos+writerIdLen]
		fileReader, err = util.NewFileReader(string(writerId))
		if err != nil {
			return nil, nil, err
		}
		pos -= len(writerId)
		err = util.UnmarshalJSON(metaBytes[0:pos], &im)
		if err != nil {
			return nil, nil, ErrorIndexMetaCorrupt
		}
	}

	if im.IndexType == "" {
		im.IndexType = upsidedown.Name
	}
	return &im, fileReader, nil
}

func (i *indexMeta) Save(path string, writer *util.FileWriter) (err error) {
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

	metaBytes, err = writer.Process(metaBytes)
	if err != nil {
		return err
	}

	_, err = indexMetaFile.Write(metaBytes)
	if err != nil {
		return err
	}

	_, err = indexMetaFile.Write([]byte(writer.Id()))
	if err != nil {
		return err
	}

	err = binary.Write(indexMetaFile, binary.BigEndian, uint32(len(writer.Id())))
	if err != nil {
		return err
	}

	return nil
}

func (i *indexMeta) CopyTo(d index.Directory) (err error) {
	metaBytes, err := util.MarshalJSON(i)
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

func (i *indexMeta) UpdateWriter(path string) error {
	indexMetaPath := indexMetaPath(path)
	metaBytes, err := os.ReadFile(indexMetaPath)
	if err != nil {
		return ErrorIndexMetaMissing
	}

	if len(metaBytes) < 4 {
		return ErrorIndexMetaCorrupt
	}

	pos := len(metaBytes) - 4
	writerIdLen := int(binary.BigEndian.Uint32(metaBytes[pos:]))
	pos -= writerIdLen
	if pos < 0 {
		return ErrorIndexMetaCorrupt
	}

	writerId := metaBytes[pos : pos+writerIdLen]
	fileReader, err := util.NewFileReader(string(writerId))
	if err != nil {
		return err
	}

	metaBytes, err = fileReader.Process(metaBytes[0:pos])
	if err != nil {
		return err
	}

	writer, err := util.NewFileWriter()
	if err != nil {
		return err
	}

	metaBytes, err = writer.Process(metaBytes)
	if err != nil {
		return err
	}

	metaBytes = append(metaBytes, []byte(writer.Id())...)
	binary.BigEndian.PutUint32(metaBytes, uint32(len(writer.Id())))
	err = os.WriteFile(indexMetaPath, metaBytes, 0666)
	if err != nil {
		return err
	}

	return nil
}

func indexMetaPath(path string) string {
	return filepath.Join(path, metaFilename)
}
