#! /usr/bin/env python3
'''
(C) 2020 Andreas Vogel
www.wellenvogel.de
MIT license

Prepare with
pip install landez
pip install python-imaging
'''

import sqlite3
import sys
import os
from PIL import Image
import io

def usage():
  print("usage: %s outfile infile [infile...]\n"%sys.argv[0])

CREATES=[
  "CREATE TABLE tiles (zoom_level integer,tile_column integer,tile_row integer, tile_data blob)",
  "CREATE TABLE metadata (name text, value text)",
  "CREATE UNIQUE INDEX name on metadata (name)",
  "CREATE UNIQUE INDEX tile_index on tiles(zoom_level, tile_column, tile_row)"
]

class Box:
  def __init__(self,minCol,maxCol,minRow,maxRow):
    self.minCol=minCol
    self.maxCol=maxCol
    self.minRow=minRow
    self.maxRow=maxRow

  def valid(self):
    if self.minCol is None:
      return False
    if self.maxCol is None:
      return False
    if self.minRow is None:
      return False
    if self.maxRow is None:
      return False
    return True

  def rowRange(self):
    return range(self.minRow,self.maxRow+1)

  def colRange(self):
    return range(self.minCol,self.maxCol+1)


def row2y(row,zoom,format="xyz"):
  if format == "xyz":
    return pow(2,zoom)-1-row
  else:
    return row

def mergeTile(tileDataStack):
  if len(tileDataStack) == 1:
    return tileDataStack[0]
  im=Image.open(io.BytesIO(tileDataStack[0])).convert("RGBA")
  for d in tileDataStack[1:]:
    if len(d) == 0:
      continue
    try:
      mi=Image.open(io.BytesIO(d)).convert("RGBA")
      im=Image.alpha_composite(im,mi)
    except Exception as e:
      print("error in overlay tile, ignore")
      continue
  out =io.BytesIO()
  im.convert("RGB").save(out,"JPEG")
  return out.getvalue()

def insertTiles(conn,stack):
  conn.executemany("insert into tiles (zoom_level,tile_column,tile_row,tile_data) values(?,?,?,?)",stack)

def mergeMbTiles(outfile,infiles):
  if len(infiles) < 1:
    usage()
    return False
  if os.path.exists(outfile):
    print("outfile %s already exists"%outfile)
    return False
  print("writing to %s"%outfile)
  #compute boxes
  minZoom=None
  maxZoom=None
  boxes={}
  for h in infiles:
    print("  adding %s"%h)
    connection=sqlite3.connect(h)
    if connection is None:
      print("unable to opn sqlite connection to %s"%h)
      return False

    cu=connection.execute("select min(zoom_level),max(zoom_level) from tiles")
    data = cu.fetchone()
    if data is not None:
      if minZoom is None or data[0] < minZoom:
        minZoom=data[0]
      if maxZoom is None or data[1] > maxZoom:
        maxZoom=data[1]
    cu.close()
    connection.close()
  print("zoom from %d to %d" % (minZoom, maxZoom))
  for zoom in range(minZoom,maxZoom+1):
    minRow=None
    maxRow=None
    minCol=None
    maxCol=None
    for h in infiles:
      connection=sqlite3.connect(h)
      if connection is None:
        print("unable to opn sqlite connection to %s"%h)
        return False
      cu=connection.execute("select min(tile_column),max(tile_column) from tiles where zoom_level=?",[zoom])
      data = cu.fetchone()
      if data is not None:
        if minCol is None or data[0] < minCol:
          minCol=data[0]
        if maxCol is None or data[1] > maxCol:
          maxCol=data[1]
      cu.close()
      cu = connection.execute("select min(tile_row),max(tile_row) from tiles where zoom_level=?", [zoom])
      data = cu.fetchone()
      if data is not None:
        if minRow is None or data[0] < minRow:
          minRow = data[0]
        if maxRow is None or data[1] > maxRow:
          maxRow = data[1]
      cu.close()
      connection.close()
    print("zoom=%d, minCol=%s,maxCol=%d,minRow=%d,maxRow=%d"%(zoom,minCol,maxCol,minRow,maxRow))
    boxes[zoom]=Box(minCol,maxCol,minRow,maxRow)

  outconnection=sqlite3.connect(outfile)
  if outconnection is None:
    print("cannot create %s"%outfile)
    return False
  outcu=outconnection.cursor()
  for stmt in CREATES:
    print("executing %s"%stmt)
    outcu.execute(stmt)
  connections=[]
  for h in infiles:
    c=sqlite3.connect(h)
    connections.append(c)
  for zoom in range(minZoom,maxZoom+1):
    box=boxes[zoom]
    if not box.valid():
      print("not tiles at zoom %d"%zoom)
      continue
    insertStack=[]
    for row in box.rowRange():
      for col in box.colRange():
        tileDataStack=[]
        for c in connections:
          cu=c.execute("select tile_data from tiles where zoom_level=? and tile_column=? and tile_row=?",[zoom,col,row])
          t=cu.fetchone()
          cu.close()
          if t is not None:
            tileDataStack.append(t[0])
        if len(tileDataStack) < 1:
          print("no tile for z=%d,r=%d,c=%d"%(zoom,row,col))
          continue
        #TODO: merge
        try:
          outdata=mergeTile(tileDataStack)
        except Exception as e:
          print("error when creating tile z=%d,row=%d,col=%d: %s",zoom,row,col,e)
          continue
        insertStack.append([zoom,col,row,outdata])
        if len(insertStack) >=10:
          insertTiles(outconnection,insertStack)
          insertStack=[]
  outconnection.commit()
  outconnection.close()






if __name__ == "__main__":
  if len(sys.argv) < 3:
    usage()
    sys.exit(1)
  mergeMbTiles(sys.argv[1],sys.argv[2:])
