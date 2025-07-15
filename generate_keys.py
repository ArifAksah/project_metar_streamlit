import bcrypt

# Daftar password yang ingin di-hash
passwords_to_hash = ['bmkgmetar', 'mnbvzxcvlkjhasdf']

hashed_passwords = []
for password in passwords_to_hash:
    # Mengubah password menjadi bytes
    password_bytes = password.encode('utf-8')
    
    # Membuat salt
    salt = bcrypt.gensalt()
    
    # Melakukan hashing
    hashed_password_bytes = bcrypt.hashpw(password_bytes, salt)
    
    # Mengubah hasil hash kembali menjadi string untuk disimpan di YAML
    hashed_passwords.append(hashed_password_bytes.decode('utf-8'))

print("--- HASHED PASSWORDS ---")
print(hashed_passwords)
print("--------------------------")
print("Salin daftar di atas ke file config.yaml Anda.")