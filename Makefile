all:
	go build -o bin/neko github.com/bigeagle/nekodb/neko
	go build -o bin/nekos github.com/bigeagle/nekodb/nekos 
	go build -o bin/nekod github.com/bigeagle/nekodb/nekod 
