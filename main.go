/*
   go-osmpbf-filter; filtering software for OpenStreetMap PBF files.
   Copyright (C) 2012  Mathieu Fenniak

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

package main

import (
	"bufio"
	"code.google.com/p/goprotobuf/proto"
	"flag"
	"fmt"
	"io"
	"os"
	"osmbpfextract/OSMPBF"
	"runtime"
	"strings"
)

type boundingBoxUpdate struct {
	wayIndex int
	lon      float64
	lat      float64
}

type node struct {
	id  int64
	lon float64
	lat float64
}

type myway struct {
	id              int64
	Name            string
	Ref             string
	nodeIds         []int64
	highway, oneway string
}

func supportedFilePass(file *os.File) {
	for data := range MakePrimitiveBlockReader(file) {
		if *data.blobHeader.Type != "OSMHeader" {
			continue
		}
		// processing OSMHeader
		blockBytes, err := DecodeBlob(data)
		if err != nil {
			println("OSMHeader blob read error:", err.Error())
			os.Exit(5)
		}

		header := &OSMPBF.HeaderBlock{}
		err = proto.Unmarshal(blockBytes, header)
		if err != nil {
			println("OSMHeader decode error:", err.Error())
			os.Exit(5)
		}

		for _, feat := range header.RequiredFeatures {
			if feat != "OsmSchema-V0.6" && feat != "DenseNodes" {
				println("Unsupported feature required in OSM header:", feat)
				os.Exit(5)
			}
		}
	}
}

func containsValue(el *string, list *[]string) bool {
	if list == nil {
		return true
	}
	for _, elx := range *list {
		if *el == elx {
			return true
		}
	}
	return false
}

func findMatchingWaysPass(file *os.File, filterTag string, filterValues []string, totalBlobCount int, output *bufio.Writer) [][]int64 {
	wayNodeRefs := make([][]int64, 0, 100)
	pending := make(chan bool)

	appendNodeRefs := make(chan []int64)
	appendNodeRefsComplete := make(chan bool)

	go func() {
		for nodeRefs := range appendNodeRefs {
			wayNodeRefs = append(wayNodeRefs, nodeRefs)
		}
		appendNodeRefsComplete <- true
	}()
	wayqueue := make(chan *myway)
	exitqueue := make(chan bool)
	done := make(chan bool)
	wCount := runtime.NumCPU() * 2

	blockDataReader := MakePrimitiveBlockReader(file)
	for i := 0; i < wCount; i++ {
		go func() {
			for data := range blockDataReader {
				if *data.blobHeader.Type == "OSMData" {
					blockBytes, err := DecodeBlob(data)
					if err != nil {
						println("OSMData decode error:", err.Error())
						os.Exit(6)
					}

					primitiveBlock := &OSMPBF.PrimitiveBlock{}
					err = proto.Unmarshal(blockBytes, primitiveBlock)
					if err != nil {
						println("OSMData decode error:", err.Error())
						os.Exit(6)
					}
					var tosend bool
					var nodeRefs []int64
					highway := ""
					oneway := ""
					ref := ""
					name := ""
					periph := false
					for _, primitiveGroup := range primitiveBlock.Primitivegroup {
						for _, way := range primitiveGroup.Ways {
							highway = ""
							oneway = ""
							ref = ""
							name = ""
							periph = false
							tosend = false
							for i, keyIndex := range way.Keys {
								valueIndex := way.Vals[i]
								key := string(primitiveBlock.Stringtable.S[keyIndex])
								value := string(primitiveBlock.Stringtable.S[valueIndex])
								if key == filterTag { // && containsValue(&value, &filterValues)
									nodeRefs = make([]int64, len(way.Refs))
									var prevNodeId int64 = 0
									for index, deltaNodeId := range way.Refs {
										nodeId := prevNodeId + deltaNodeId
										prevNodeId = nodeId
										nodeRefs[index] = nodeId
									}
									highway = value
									tosend = true
									appendNodeRefs <- nodeRefs
								}
								if key == "oneway" {
									oneway = value
								}
								if key == "name" {
									name = value
									if value == "Boulevard Périphérique Intérieur" || value == "Boulevard Périphérique Extérieur" {
										periph = true
									}
								}
								if key == "ref" {
									ref = value
								}
							}
							if tosend {
								if periph {
									highway = "peripherique"
								}
								wayqueue <- &myway{id: *way.Id, Name: strings.Replace(name, ",", "", -1), Ref: ref, nodeIds: nodeRefs, highway: highway, oneway: oneway}
							}
						}
					}
				}
				pending <- true
			}
			exitqueue <- true
		}()
	}

	go func() {
		j := 0
		for i := 0; true; i++ {
			select {
			case way := <-wayqueue:
				csv := make([]string, len(way.nodeIds))
				for i, v := range way.nodeIds {
					csv[i] = fmt.Sprintf("%d", v)
				}
				output.WriteString(fmt.Sprintf("%d,%s,%s,%s,%s,%s\n", way.id, way.Name, way.Ref, way.highway, way.oneway, strings.Join(csv, ",")))
				if i%1000 == 0 {
					output.Flush()
				}
			case <-exitqueue:
				j++
				output.Flush()
				fmt.Println("Work return, ", i, "nodes processed")
				if j == wCount {
					done <- true
					return
				}
			}
		}
	}()

	blobCount := 0
	for _ = range pending {
		blobCount += 1
		if blobCount%500 == 0 {
			println("\tComplete:", blobCount, "\tRemaining:", totalBlobCount-blobCount)
		}
		if blobCount == totalBlobCount {
			close(pending)
			close(appendNodeRefs)
			<-appendNodeRefsComplete
			close(appendNodeRefsComplete)
		}
	}

	<-done
	return wayNodeRefs
}

func findMatchingNodesPass(file *os.File, wayNodeRefs [][]int64, totalBlobCount int, output *bufio.Writer) {
	// maps node ids to wayNodeRef indexes
	nodeOwners := make(map[int64]bool) // len(wayNodeRefs)*2)
	for _, way := range wayNodeRefs {
		for _, nodeId := range way {
			nodeOwners[nodeId] = true
		}
	}
	pending := make(chan bool)

	blockDataReader := MakePrimitiveBlockReader(file)

	nodequeue := make(chan *node)
	exitqueue := make(chan bool)
	done := make(chan bool)

	wCount := runtime.NumCPU() * 2
	for i := 0; i < wCount; i++ {
		go func() {
			//var n node
			var lon, lat float64
			for data := range blockDataReader {
				if *data.blobHeader.Type == "OSMData" {
					blockBytes, err := DecodeBlob(data)
					if err != nil {
						println("OSMData decode error:", err.Error())
						os.Exit(6)
					}

					primitiveBlock := &OSMPBF.PrimitiveBlock{}
					err = proto.Unmarshal(blockBytes, primitiveBlock)
					if err != nil {
						println("OSMData decode error:", err.Error())
						os.Exit(6)
					}

					for absNode := range MakeNodeReader(primitiveBlock) {
						owners := nodeOwners[absNode.GetNodeId()]
						if owners == false {
							continue
						}
						lon, lat = absNode.GetLonLat()

						nodequeue <- &node{absNode.GetNodeId(), lon, lat}
					}
				}
				pending <- true
			}
			exitqueue <- true
		}()
	}

	go func() {
		j := 0
		for i := 0; true; i++ {
			select {
			case n := <-nodequeue:
				output.WriteString(fmt.Sprintf("%d,%f,%f\n", (*n).id, (*n).lat, (*n).lon))
				if i%1000 == 0 {
					output.Flush()
				}
			case <-exitqueue:
				j++
				output.Flush()
				fmt.Println("Work return, ", i, "nodes processed")
				if j == wCount {
					done <- true
					return
				}
			}
		}
	}()

	blobCount := 0
	for _ = range pending {
		blobCount += 1
		if blobCount%500 == 0 {
			println("\tComplete:", blobCount, "\tRemaining:", totalBlobCount-blobCount)
		}
		if blobCount == totalBlobCount {
			close(pending)
		}
	}
	<-done
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU() * 2)

	inputFile := flag.String("i", "input.pbf.osm", "input OSM PBF file")
	highMemory := flag.Bool("high-memory", false, "use higher amounts of memory for higher performance")
	filterTag := flag.String("t", "highway", "tag to filter ways based upon")
	filterValString := flag.String("r", "motorway motorway_link trunk trunk_link primary primary_link secondary secondary_link tertiary tertiary_link", "types of roads to import")
	flag.Parse()

	mystrings := strings.Fields(*filterValString)
	filterValues := &mystrings
	fmt.Println("Will find", *filterTag, "for", mystrings)

	file, err := os.Open(*inputFile)
	if err != nil {
		println("Unable to open file:", err.Error())
		os.Exit(1)
	}

	// Count the total number of blobs; provides a nice progress indicator
	totalBlobCount := 0
	for {
		blobHeader, err := ReadNextBlobHeader(file)
		if err == io.EOF {
			break
		} else if err != nil {
			println("Blob header read error:", err.Error())
			os.Exit(2)
		}

		totalBlobCount += 1
		file.Seek(int64(*blobHeader.Datasize), os.SEEK_CUR)
	}
	println("Total number of blobs:", totalBlobCount)

	if *highMemory {
		cacheUncompressedBlobs = make(map[int64][]byte, totalBlobCount)
	}

	println("Pass 1/3: Find OSMHeaders")
	supportedFilePass(file)
	println("Pass 1/3: Complete")

	// saving ways in a csv file

	waysfile, err := os.OpenFile("ways.csv", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0664)
	if err != nil {
		println("Output file write error:", err.Error())
		os.Exit(2)
	}

	println("Pass 2/3: Find node references of matching areas")
	wayNodeRefs := findMatchingWaysPass(file, *filterTag, *filterValues, totalBlobCount, bufio.NewWriter(waysfile))
	println("Pass 2/3: Complete;", len(wayNodeRefs), "matching ways found.")

	nodesfile, err := os.OpenFile("nodes.csv", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0664)
	if err != nil {
		println("Output file write error:", err.Error())
		os.Exit(2)
	}

	println("Pass 3/3: Finding nodes")
	findMatchingNodesPass(file, wayNodeRefs, totalBlobCount, bufio.NewWriter(nodesfile))
	println("Pass 3/3: Complete; nodes recorded.")

}
