#!/bin/bash

# Generate Test TLS Certificates for DistCacheLayer
# This script creates a CA and server/client certificates for testing
# DO NOT use these certificates in production!

set -e

CERT_DIR="certs"
DAYS_VALID=365

echo "========================================="
echo "  DistCacheLayer Certificate Generator"
echo "========================================="
echo ""
echo "Creating test certificates in: $CERT_DIR"
echo "Valid for: $DAYS_VALID days"
echo ""
echo "⚠️  WARNING: These are TEST certificates only!"
echo "⚠️  DO NOT use in production!"
echo ""

# Create cert directory
mkdir -p "$CERT_DIR"
cd "$CERT_DIR"

# Clean up old certificates
echo "Cleaning up old certificates..."
rm -f *.crt *.key *.csr *.srl *.pem

# 1. Generate CA (Certificate Authority)
echo ""
echo "Step 1: Generating CA certificate..."
openssl genrsa -out ca.key 4096
openssl req -new -x509 -key ca.key -sha256 -days $DAYS_VALID -out ca.crt \
    -subj "/C=US/ST=California/L=San Francisco/O=DistCacheLayer/OU=Testing/CN=DistCache Test CA"
echo "✅ CA certificate created: ca.crt"

# 2. Generate Server Certificate
echo ""
echo "Step 2: Generating server certificate..."
openssl genrsa -out server.key 2048
openssl req -new -key server.key -out server.csr \
    -subj "/C=US/ST=California/L=San Francisco/O=DistCacheLayer/OU=Server/CN=localhost"

# Create server extensions file for SAN (Subject Alternative Name)
cat > server.ext << EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
DNS.2 = distcache-node1
DNS.3 = distcache-node2
DNS.4 = distcache-node3
DNS.5 = distcache-node4
DNS.6 = distcache-node5
DNS.7 = coordinator
IP.1 = 127.0.0.1
IP.2 = ::1
EOF

openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
    -out server.crt -days $DAYS_VALID -sha256 -extfile server.ext
echo "✅ Server certificate created: server.crt"

# 3. Generate Client Certificate
echo ""
echo "Step 3: Generating client certificate..."
openssl genrsa -out client.key 2048
openssl req -new -key client.key -out client.csr \
    -subj "/C=US/ST=California/L=San Francisco/O=DistCacheLayer/OU=Client/CN=distcache-client"

# Create client extensions file
cat > client.ext << EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
extendedKeyUsage = clientAuth
EOF

openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
    -out client.crt -days $DAYS_VALID -sha256 -extfile client.ext
echo "✅ Client certificate created: client.crt"

# 4. Generate Coordinator Certificate
echo ""
echo "Step 4: Generating coordinator certificate..."
openssl genrsa -out coordinator.key 2048
openssl req -new -key coordinator.key -out coordinator.csr \
    -subj "/C=US/ST=California/L=San Francisco/O=DistCacheLayer/OU=Coordinator/CN=coordinator"

cat > coordinator.ext << EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
subjectAltName = @alt_names

[alt_names]
DNS.1 = coordinator
DNS.2 = localhost
IP.1 = 127.0.0.1
EOF

openssl x509 -req -in coordinator.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
    -out coordinator.crt -days $DAYS_VALID -sha256 -extfile coordinator.ext
echo "✅ Coordinator certificate created: coordinator.crt"

# 5. Clean up temporary files
echo ""
echo "Step 5: Cleaning up temporary files..."
rm -f *.csr *.srl *.ext
echo "✅ Cleanup complete"

# 6. Set appropriate permissions
echo ""
echo "Step 6: Setting file permissions..."
chmod 600 *.key
chmod 644 *.crt
echo "✅ Permissions set"

# 7. Create PEM bundles (certificate + key in one file)
echo ""
echo "Step 7: Creating PEM bundles..."
cat server.crt server.key > server.pem
cat client.crt client.key > client.pem
cat coordinator.crt coordinator.key > coordinator.pem
chmod 600 *.pem
echo "✅ PEM bundles created"

# Summary
echo ""
echo "========================================="
echo "  Certificate Generation Complete! ✅"
echo "========================================="
echo ""
echo "Generated files in $CERT_DIR/:"
echo "  📄 ca.crt          - CA certificate (trust this on clients)"
echo "  🔑 ca.key          - CA private key (keep secure!)"
echo "  📄 server.crt      - Server certificate"
echo "  🔑 server.key      - Server private key"
echo "  📦 server.pem      - Server bundle (cert + key)"
echo "  📄 client.crt      - Client certificate"
echo "  🔑 client.key      - Client private key"
echo "  📦 client.pem      - Client bundle (cert + key)"
echo "  📄 coordinator.crt - Coordinator certificate"
echo "  🔑 coordinator.key - Coordinator private key"
echo "  📦 coordinator.pem - Coordinator bundle (cert + key)"
echo ""
echo "Certificate Details:"
openssl x509 -in server.crt -noout -subject -issuer -dates
echo ""
echo "Verify Certificates:"
echo "  Server:      openssl verify -CAfile ca.crt server.crt"
echo "  Client:      openssl verify -CAfile ca.crt client.crt"
echo "  Coordinator: openssl verify -CAfile ca.crt coordinator.crt"
echo ""
echo "⚠️  Remember: These are TEST certificates only!"
echo "⚠️  Generate proper certificates for production use!"
echo ""
echo "Next steps:"
echo "  1. Update config files to use these certificates"
echo "  2. Configure server to use server.crt and server.key"
echo "  3. Configure clients to trust ca.crt"
echo "  4. Test TLS connections"
echo ""

cd ..
