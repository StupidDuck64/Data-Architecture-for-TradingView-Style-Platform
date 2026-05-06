# Tóm tắt Lỗi và Nguyên Nhân Gốc Rễ

## 🔴 Các lỗi gặp phải khi chạy dự án

### 1. **Exec format error** (Spark, Trino, Nginx)

**Lỗi:**
```
exec /entrypoint.sh: exec format error
```

**Nguyên nhân:**

#### A. Line endings khác nhau (CRLF vs LF)
- **Windows**: Sử dụng CRLF (`\r\n` - Carriage Return + Line Feed)
- **Linux/Docker**: Sử dụng LF (`\n` - Line Feed only)
- **Vấn đề**: Khi shell script có CRLF, Linux shell không nhận diện được shebang `#!/bin/bash\r` → lỗi exec format

#### B. BOM (Byte Order Mark)
- **UTF-8 BOM**: 3 bytes `\xEF\xBB\xBF` ở đầu file
- **Vấn đề**: Shell interpreter đọc `\xEF\xBB\xBF#!/bin/bash` thay vì `#!/bin/bash` → không tìm thấy interpreter

#### C. Docker build context sai
- **Cấu hình ban đầu**:
  ```yaml
  spark-master:
    build:
      context: ./docker/spark  # ❌ Sai
  ```
- **Vấn đề**: Dockerfile có `COPY entrypoint.sh /entrypoint.sh` nhưng file nằm ở `./docker/spark/entrypoint.sh` → relative path từ context `./docker/spark` không tìm thấy

---

## 💡 Tại sao máy người ta chạy được mà máy bạn không?

### Scenario 1: Developer trên Linux/macOS
```bash
# Trên Linux/macOS
$ file docker/spark/entrypoint.sh
docker/spark/entrypoint.sh: Bourne-Again shell script, ASCII text executable

# Line endings tự nhiên là LF
$ od -c docker/spark/entrypoint.sh | head -1
0000000   #   !   /   b   i   n   /   b   a   s   h  \n
```

### Scenario 2: Developer trên Windows với Git config đúng
```bash
# Git config
$ git config core.autocrlf
input  # Convert CRLF→LF khi commit, giữ LF khi checkout

# Hoặc có .gitattributes
$ cat .gitattributes
*.sh text eol=lf
```

### Scenario 3: Bạn trên Windows với Git config mặc định
```bash
# Git config mặc định trên Windows
$ git config core.autocrlf
true  # Convert LF→CRLF khi checkout, CRLF→LF khi commit

# Kết quả sau khi git clone
$ file docker/spark/entrypoint.sh
docker/spark/entrypoint.sh: Bourne-Again shell script, ASCII text executable, with CRLF line terminators

# Line endings bị convert thành CRLF
$ od -c docker/spark/entrypoint.sh | head -1
0000000   #   !   /   b   i   n   /   b   a   s   h  \r  \n
                                                    ^^^^
```

### Scenario 4: Editor thêm BOM
```bash
# VS Code hoặc Notepad++ có thể save file với UTF-8 BOM
$ hexdump -C docker/trino/entrypoint.sh | head -1
00000000  ef bb bf 23 21 2f 62 69  6e 2f 62 61 73 68 0a 0a  |...#!/bin/bash..|
          ^^^^^^^^
          UTF-8 BOM
```

---

## 🛠️ Giải pháp đã áp dụng

### 1. Convert line endings (CRLF → LF)
```bash
# Sử dụng dos2unix
dos2unix docker/spark/entrypoint.sh
dos2unix docker/trino/entrypoint.sh
dos2unix docker/nginx/entrypoint.sh
dos2unix scripts/nginx_auto_reload.sh

# Hoặc sed
sed -i 's/\r$//' docker/spark/entrypoint.sh
```

### 2. Remove BOM
```bash
# Remove UTF-8 BOM (3 bytes đầu tiên)
sed -i '1s/^\xEF\xBB\xBF//' docker/trino/entrypoint.sh
sed -i '1s/^\xEF\xBB\xBF//' docker/nginx/entrypoint.sh
```

### 3. Fix Docker build context
```yaml
# docker-compose.yml - BEFORE
spark-master:
  build:
    context: ./docker/spark  # ❌ Chỉ có thể access files trong ./docker/spark/

# docker-compose.yml - AFTER
spark-master:
  build:
    context: .  # ✅ Access toàn bộ project root
    dockerfile: docker/spark/Dockerfile
```

### 4. Update Dockerfile paths
```dockerfile
# docker/spark/Dockerfile - BEFORE
COPY entrypoint.sh /entrypoint.sh  # ❌ Tìm ./entrypoint.sh (không tồn tại)

# docker/spark/Dockerfile - AFTER
COPY docker/spark/entrypoint.sh /entrypoint.sh  # ✅ Tìm từ project root
```

---

## 🛡️ Cách phòng tránh cho dự án (Best Practices)

### 1. Git Configuration

#### A. `.gitattributes` (Bắt buộc)
```bash
# Tạo file .gitattributes ở project root
cat > .gitattributes << 'EOF'
# Force LF for shell scripts
*.sh text eol=lf

# Force LF for Dockerfiles
Dockerfile text eol=lf
*.Dockerfile text eol=lf

# Force LF for Python
*.py text eol=lf

# Force LF for config files
*.conf text eol=lf
*.yaml text eol=lf
*.yml text eol=lf

# Binary files
*.png binary
*.jpg binary
*.woff binary
*.woff2 binary
EOF

# Commit
git add .gitattributes
git commit -m "Add .gitattributes to enforce LF line endings"
```

#### B. Git config cho developer
```bash
# Trên Windows, set autocrlf = input (không convert khi checkout)
git config --global core.autocrlf input

# Hoặc false (không convert gì cả)
git config --global core.autocrlf false
```

### 2. EditorConfig

```bash
# Tạo .editorconfig ở project root
cat > .editorconfig << 'EOF'
root = true

[*]
charset = utf-8
end_of_line = lf
insert_final_newline = true
trim_trailing_whitespace = true

[*.sh]
end_of_line = lf
indent_style = space
indent_size = 4

[*.{py,js,ts,tsx}]
indent_style = space
indent_size = 4

[*.{yaml,yml}]
indent_style = space
indent_size = 2

[Dockerfile*]
end_of_line = lf
EOF
```

### 3. Pre-commit Hook

```bash
# Tạo .git/hooks/pre-commit
cat > .git/hooks/pre-commit << 'EOF'
#!/bin/bash

echo "Checking for CRLF line endings in shell scripts..."

# Get list of staged .sh files
STAGED_SH_FILES=$(git diff --cached --name-only --diff-filter=ACM | grep '\.sh$')

if [ -n "$STAGED_SH_FILES" ]; then
    # Check for CRLF
    if echo "$STAGED_SH_FILES" | xargs grep -l $'\r' > /dev/null 2>&1; then
        echo "❌ ERROR: Shell scripts contain CRLF line endings:"
        echo "$STAGED_SH_FILES" | xargs grep -l $'\r'
        echo ""
        echo "Fix with: dos2unix <file>"
        exit 1
    fi
    
    # Check for BOM
    if echo "$STAGED_SH_FILES" | xargs grep -l $'^\xEF\xBB\xBF' > /dev/null 2>&1; then
        echo "❌ ERROR: Shell scripts contain UTF-8 BOM:"
        echo "$STAGED_SH_FILES" | xargs grep -l $'^\xEF\xBB\xBF'
        echo ""
        echo "Fix with: sed -i '1s/^\xEF\xBB\xBF//' <file>"
        exit 1
    fi
fi

echo "✅ All shell scripts are clean"
exit 0
EOF

chmod +x .git/hooks/pre-commit
```

### 4. CI/CD Validation

```yaml
# .github/workflows/validate-line-endings.yml
name: Validate Line Endings

on: [push, pull_request]

jobs:
  check-line-endings:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Check for CRLF in shell scripts
        run: |
          if find . -name "*.sh" -type f -exec grep -l $'\r' {} \; | grep .; then
            echo "❌ Found CRLF line endings in shell scripts"
            exit 1
          fi
          echo "✅ All shell scripts have LF line endings"
      
      - name: Check for BOM
        run: |
          if find . -name "*.sh" -type f -exec grep -l $'^\xEF\xBB\xBF' {} \; | grep .; then
            echo "❌ Found UTF-8 BOM in shell scripts"
            exit 1
          fi
          echo "✅ No BOM found"
```

### 5. Dockerfile Best Practices

```dockerfile
# Luôn convert line endings trong Dockerfile
FROM alpine:latest

# Install dos2unix
RUN apk add --no-cache dos2unix

# Copy và convert
COPY docker/spark/entrypoint.sh /entrypoint.sh
RUN dos2unix /entrypoint.sh && \
    chmod +x /entrypoint.sh && \
    rm -rf /var/cache/apk/*

ENTRYPOINT ["/entrypoint.sh"]
```

### 6. Documentation

```markdown
# README.md - Setup Instructions

## Windows Developers

### 1. Configure Git
```bash
git config --global core.autocrlf input
```

### 2. Install dos2unix
```bash
# Via Chocolatey
choco install dos2unix

# Via Git Bash (included)
# dos2unix is available in Git Bash
```

### 3. Convert existing files (if needed)
```bash
find . -name "*.sh" -type f -exec dos2unix {} \;
```

### 4. VS Code Settings
```json
{
  "files.eol": "\n",
  "files.encoding": "utf8"
}
```
```

---

## 📊 So sánh trước và sau

### Trước khi fix

```bash
$ docker compose up -d
...
spark-master  | exec /entrypoint.sh: exec format error
trino         | exec /usr/local/bin/entrypoint.sh: exec format error
nginx         | exec /entrypoint.sh: exec format error

$ docker compose ps
NAME          STATUS
spark-master  Restarting (255)
trino         Restarting (255)
nginx         Restarting (255)
```

### Sau khi fix

```bash
$ docker compose up -d
...
✓ Container spark-master  Started
✓ Container trino         Started
✓ Container nginx         Started

$ docker compose ps
NAME          STATUS
spark-master  Up 2 minutes
trino         Up 2 minutes (healthy)
nginx         Up 2 minutes (healthy)

$ curl -s https://localhost/ | grep title
    <title>LMView</title>
```

---

## 🎯 Checklist cho dự án mới

- [ ] Tạo `.gitattributes` với `*.sh text eol=lf`
- [ ] Tạo `.editorconfig` với `end_of_line = lf`
- [ ] Setup pre-commit hook kiểm tra CRLF
- [ ] Document Git config cho Windows developers
- [ ] Thêm CI/CD validation
- [ ] Dockerfile có dos2unix cho shell scripts
- [ ] Docker build context = project root
- [ ] Test trên cả Windows và Linux

---

## 📚 Tài liệu tham khảo

- [Git - gitattributes Documentation](https://git-scm.com/docs/gitattributes)
- [EditorConfig](https://editorconfig.org/)
- [Docker Best Practices](https://docs.docker.com/develop/dev-best-practices/)
- [Bash Shebang](https://en.wikipedia.org/wiki/Shebang_(Unix))
