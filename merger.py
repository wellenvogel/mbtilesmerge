#! /usr/bin/env python3
'''
(C) 2020 Andreas Vogel
www.wellenvogel.de
MIT license

Prepare with
pip install pillow
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
  def __init__(self,minCol=None,maxCol=None,minRow=None,maxRow=None):
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

def mergeTile(tileDataStack,format):
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
  im.convert("RGB").save(out,format)
  return out.getvalue()

def insertTiles(conn,stack):
  conn.executemany("insert into tiles (zoom_level,tile_column,tile_row,tile_data) values(?,?,?,?)",stack)

def getTileFormat(conn):
  row=conn.execute("select tile_data from tiles limit 10")
  format =None
  while format is None:
    t=row.fetchone()
    if t is None:
      row.close()
      return None
    if  len(t[0]) > 0:
      img=Image.open(io.BytesIO(t[0]))
      if img.format is not None:
        return img.format

def fetchTile(connection,col,row,zoom):
  cu = connection.execute("select tile_data from tiles where zoom_level=? and tile_column=? and tile_row=?", [zoom, col, row])
  t = cu.fetchone()
  cu.close()
  if t is None:
    return None
  return t[0]

def mergeMbTiles(outfile,infiles):
  if len(infiles) < 1:
    usage()
    return False
  if os.path.exists(outfile):
    print("outfile %s already exists"%outfile)
    return False
  for f in infiles:
    if not os.path.exists(f):
      print("infile %s not found"%f)
      return False
  print("writing to %s"%outfile)
  #compute boxes
  minZoom=None
  maxZoom=None
  boxes={}
  h=infiles[0]
  print("  baselayer %s"%h)
  connection=sqlite3.connect(h)
  if connection is None:
    print("unable to open sqlite connection to %s"%h)
    return False
  zoomlevels=[]
  cu=connection.execute("select distinct zoom_level from tiles")
  for z in cu.fetchall():
    zoomlevels.append(z[0])
  cu.close()
  if len(zoomlevels) < 1:
    print("no zoomlevels found in %s"%h)
    return False
  print("zoom levels in %s: %s" % (h, ",".join(map(lambda x: str(x),zoomlevels))))
  for zoom in zoomlevels:
    box=Box()
    cu=connection.execute("select min(tile_column),max(tile_column) from tiles where zoom_level=?",[zoom])
    data = cu.fetchone()
    if data is not None:
      box.minCol=data[0]
      box.maxCol=data[1]
    cu.close()
    cu = connection.execute("select min(tile_row),max(tile_row) from tiles where zoom_level=?", [zoom])
    data = cu.fetchone()
    if data is not None:
      box.minRow=data[0]
      box.maxRow=data[1]
    cu.close()
    print("zoom=%d, minCol=%s,maxCol=%d,minRow=%d,maxRow=%d"%(zoom,box.minCol,box.maxCol,box.minRow,box.maxRow))
    boxes[zoom]=box
  format=getTileFormat(connection)
  if format is None:
    print("unable to determine tile format for base layer %s"%h)
    return False
  print("base layer tile format is %s"%format)
  outconnection=sqlite3.connect(outfile)
  if outconnection is None:
    print("cannot create %s"%outfile)
    return False
  outcu=outconnection.cursor()
  for stmt in CREATES:
    print("executing %s"%stmt)
    outcu.execute(stmt)
  connections=[]
  for ov in infiles[1:]:
    print("adding %s"%ov)
    c=sqlite3.connect(ov)
    connections.append(c)
  for zoom in zoomlevels:
    box=boxes[zoom]
    if not box.valid():
      print("not tiles at zoom %d"%zoom)
      continue
    insertStack=[]
    for row in box.rowRange():
      for col in box.colRange():
        tileDataStack=[]
        base=fetchTile(connection,col,row,zoom)
        if base is None or len(base) == 0:
          print("no tile for z=%d,r=%d,c=%d" % (zoom, row, col))
          continue
        tileDataStack.append(base)
        for c in connections:
          t=fetchTile(c,col,row,zoom)
          if t is not None:
            tileDataStack.append(t)
        try:
          outdata=mergeTile(tileDataStack,format)
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
