# shardsqlite

Proyek eksperimen untuk mereplikasi SQLite memakai Raft. Log Raft menjadi sumber kebenaran. Frame WAL SQLite menjadi salinan halaman lokal dari entri Raft yang sudah komit. Keduanya tidak dianggap sebagai log yang sama.

Proyek ini belum siap untuk produksi. Pembacaan bisa tertunda. Fungsi sharding belum ada, tetapi penanda group dan shard sudah ikut dalam setiap batch replikasi.

Dokumen lain: [README bahasa Inggris](README.md).

## Cara kerja

1. Aplikasi melakukan commit lewat VFS SQLite khusus.
2. VFS menahan frame WAL dan menunda commit.
3. Leader mengirim frame itu ke Raft.
4. Setelah quorum, FSM menulis frame ke berkas `-wal` di setiap node.
5. Pembaruan wal-index `-shm` langsung membuat frame itu terlihat oleh pembaca yang sudah terbuka.
6. Checkpoint dan reset WAL tetap lokal, tetapi header WAL baru ikut dalam batch berikutnya.

Hanya leader Raft yang menerima tulisan. Follower bersifat hanya baca.

## Komponen

| Jalur | Fungsi |
| --- | --- |
| `internal/raftvfs` | Gerbang commit VFS SQLite dan pengambilan frame WAL |
| `internal/raftlog` | Penyandian batch, pembagian batch, dan posisi WAL |
| `internal/cluster` | Node Raft, FSM, snapshot, restore, dan gerbang baca |
| `internal/replay` | Pemutaran frame WAL dan pembaruan wal-index `-shm` |
| `internal/applied` | Penanda applied index dan generasi WAL di tiap node |
| `internal/checkpoint` | Checkpoint lokal yang dijalankan manual |
| `internal/wal` | Header WAL, frame, dan checksum |
| `cmd/shardsqlited` | Program node |
| `spec` | Model state TLA+ dan konfigurasi TLC |
| `properties` | Pemeriksaan properti trust-gateway |

## Bangun dan jalankan

Kamu perlu Go 1.26 atau yang lebih baru.

```sh
go build ./cmd/shardsqlited
./shardsqlited \
  -node-id=1 \
  -data-dir=./data-node-1 \
  -db=./shard-node-1.db \
  -bind=127.0.0.1:9200 \
  -vfs=shardsqlite-node-1
```

Untuk klaster tiga node, berikan daftar peer yang sama ke setiap program:

```sh
./shardsqlited \
  -node-id=1 \
  -data-dir=./data-node-1 \
  -db=./shard-node-1.db \
  -bind=127.0.0.1:9200 \
  -vfs=shardsqlite-node-1 \
  -peers=1@127.0.0.1:9200,2@127.0.0.1:9201,3@127.0.0.1:9202
```

Setiap node harus mulai dari byte berkas database utama yang sama. Frame WAL tidak bisa membangun database kosong.

Aplikasi membuka database memakai VFS yang sudah didaftarkan:

```text
file:/path/shard.db?vfs=shardsqlite-node-1&_pragma=journal_mode(WAL)&_pragma=wal_autocheckpoint(0)&_pragma=synchronous(FULL)
```

## Pembacaan

Pembacaan bisa tertunda. Hasil baca mengikuti applied index pada node yang kamu baca.

Kamu tidak mendapat jaminan langsung membaca tulisan sendiri. Aplikasi harus mengarahkan pembacaan ke leader atau menunggu applied index.

Pembacaan linearizable tidak masuk cakupan saat ini. `hashicorp/raft` v1.8 tidak menyediakan API read-index.

## Verifikasi

`trust-gateway` menjalankan semua skrip `properties/<name>/check`:

```sh
trust-gateway
```

Pemeriksaan yang ada mencakup checksum WAL, applied index yang monoton, pembagian batch, commit yang menunggu quorum, replikasi tiga node, snapshot dan restore, pembacaan tertunda, checkpoint lokal, pembaruan `-shm` langsung, pemulihan setelah restart, propagasi reset WAL, dan model TLA+ terbatas.

Pemeriksaan Go:

```sh
gofmt -l .
go vet ./...
go build ./...
go test ./... -race
```

Model TLA+ ada di `spec/ShardSQLite.tla`. Pemeriksaan trust-gateway memakai `spec/ShardSQLiteSmall.cfg`. Konfigurasi itu memakai 3 node, `MaxLog = 3`, dan `MaxPending = 1`. TLC tidak menemukan pelanggaran invariant pada model terbatas itu. Ini bukan bukti untuk jumlah state tak terbatas.

## Batasan

- Satu group Raft menguasai satu shard. Logika sharding belum ada.
- Setiap batch hanya menargetkan satu database SQLite. Transaksi lintas shard tidak didukung.
- `VACUUM`, perubahan `page_size`, dan perubahan `journal_mode` tidak dirancang untuk replikasi.
- Restore snapshot perlu koordinasi dengan koneksi aplikasi yang masih terbuka.
- Pembaca yang sudah terbuka melihat frame baru lewat pembaruan wal-index `-shm` pada jalur apply.

Lihat [PLAN.md](PLAN.md) untuk rancangan, keputusan, dan bagian yang masih terbuka.
