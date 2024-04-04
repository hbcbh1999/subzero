/*
licenses keys are generated like this:
openssl ecparam -name prime256v1 -genkey -noout -out ecdsa_p256_private.pem
openssl ec -in ecdsa_p256_private.pem -pubout -out ecdsa_p256_public.pem
for rust tests, the private key in psk format can be optained with
openssl pkcs8 -topk8 -nocrypt -in ecdsa_p256_private.pem -out pkcs8_private.pem

license key is generated in js using this code
export function getLicenseData(email: string, plan: string, exp: number): any {
    return {email,plan,exp};
}

export async function generateLicenseKey(data: any, privateKey:CryptoKey): Promise<string> {
    const encoder = new TextEncoder();
    const dataStr = JSON.stringify(data);
    const dataUint8 = encoder.encode(dataStr);

    // Assuming you have the ECDSA P-256 keys already generated and the privateKey is available
    const signature = await crypto.subtle.sign(
        { name: "ECDSA", hash: { name: "SHA-256" } },
        privateKey, // from your secure storage
        dataUint8,
    );

    const signatureBase64 = arrayBufferToBase64(signature);
    const dataBase64 = arrayBufferToBase64(dataUint8);

    return `${dataBase64}.${signatureBase64}`;
}
*/

use ring::signature::UnparsedPublicKey;
use base64::prelude::*;
use serde_json::Value;
use std::str::from_utf8;

#[derive(Debug, Clone)]
pub struct LicenseData {
    pub email: String,
    pub plan: String,
    pub exp: i64,
}

fn parse_pem(in_pem: &str) -> Result<Vec<u8>, pem::PemError> {
    let pem_p = pem::parse(in_pem)?;
    let pem_contents = pem_p.contents();
    Ok(pem_contents.to_vec())
}
fn extract_public_key(der: Vec<u8>) -> Result<Vec<u8>, yasna::ASN1Error> {
    let asn = yasna::parse_der(&der, |reader| {
        reader.read_sequence(|reader| {
            reader.next().read_sequence(|info_reader| {
                info_reader.next().read_oid()?;
                info_reader.next().read_oid()?;
                Ok(())
            })?;
            let b = reader.next().read_bitvec_bytes()?;
            Ok(b)
        })
    })?;

    let public_key = asn.0;
    Ok(public_key)
}
pub fn get_license_info(license_key: &str, public_key: &str, current_timestamp: i64) -> Result<LicenseData, &'static str> {
    let parts: Vec<&str> = license_key.trim().split('.').collect();
    if parts.len() != 2 {
        return Err("Invalid license key");
    }
    let data = BASE64_STANDARD.decode(parts[0]).map_err(|_| "Failed to decode license key")?;
    let signature = BASE64_STANDARD.decode(parts[1]).map_err(|_| "Failed to decode license key")?;
    let pem_der = parse_pem(public_key).map_err(|_| "Failed to parse public key pem")?;
    let public_key_der = extract_public_key(pem_der).map_err(|_| "Failed to extract public key")?;
    let public_key = UnparsedPublicKey::new(&ring::signature::ECDSA_P256_SHA256_FIXED, &public_key_der);
    let check = public_key.verify(&data, &signature);
    let is_valid = check.is_ok();
    if !is_valid {
        return Err("Invalid license key");
    }
    let data_str = from_utf8(&data).map_err(|_| "Invalid license key (utf8)")?;
    let data: Value = serde_json::from_str(data_str).map_err(|_| "Invalid license key (json)")?;
    let email = data["email"].as_str().ok_or("Invalid license key (email)")?.to_string();
    let plan = data["plan"].as_str().ok_or("Invalid license key (plan)")?.to_string();
    let exp = data["exp"].as_i64().ok_or("Invalid license key (exp)")?;

    if exp < current_timestamp {
        return Err("License expired");
    }

    Ok(LicenseData { email, plan, exp })
}

// #[cfg(test)]
// mod tests {

//     use super::*;
//     static PUBLIC_LICENSE_PEM: &str = include_str!("../../../subzero-license-server/ecdsa_p256_public.pem");
//     //static PRIVATE_LICENSE_PEM: &str = include_str!("../../../subzero-license-server/ecdsa_p256_private.pem");
//     static PRIVATE_LICENSE_PEM: &str = include_str!("../../../subzero-license-server/pkcs8_private.pem");
//     // static PRIVATE_LICENSE_PEM: &str = include_str!("../../../subzero-license-server/ecdsa_p256_private_pkcs8.pem");

//     #[test]
//     fn check_license_key_decoding() {
//         let email = "me@my.com";
//         let plan = "free";
//         let exp = 2000000000;
//         let data = format!("{{\"email\":\"{}\",\"plan\":\"{}\",\"exp\":{}}}", email, plan, exp);
//         println!("starting test");
//         let rng = ring::rand::SystemRandom::new();
//         let private_key_der = parse_pem(PRIVATE_LICENSE_PEM).unwrap();
//         // let public_key_der = extract_public_key(parse_pem(PUBLIC_LICENSE_PEM).unwrap()).unwrap();
//         // let keypair = ring::signature::EcdsaKeyPair::from_private_key_and_public_key(
//         //     &ring::signature::ECDSA_P256_SHA256_FIXED_SIGNING,
//         //     &private_key_der,
//         //     &public_key_der,
//         //     &rng
//         // );
//         let keypair = ring::signature::EcdsaKeyPair::from_pkcs8(
//             &ring::signature::ECDSA_P256_SHA256_FIXED_SIGNING,
//             &private_key_der,
//             &rng
//         );
//         let keypair = keypair.unwrap();
//         let signature = keypair.sign(&ring::rand::SystemRandom::new(), data.as_bytes()).unwrap();
//         let signature_base64 = BASE64_STANDARD.encode(signature.as_ref());
//         let data_base64 = BASE64_STANDARD.encode(data.as_bytes());
//         let license_key = format!("{}.{}", data_base64, signature_base64);
//         println!("data_base64: {}", data_base64);
//         println!("signature_base64: {}", signature_base64);
//         println!("license_key: {}", license_key);
//         let license_data = get_license_info(&license_key, PUBLIC_LICENSE_PEM).unwrap();
//         assert_eq!(license_data.email, email);
//     }
// }
