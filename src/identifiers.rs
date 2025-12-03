use std::borrow::Cow;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BusIdentifiers {
    pub redis_hash: String,
    pub control_topic: String,
    pub measurement_topic: String,
}

pub fn derive_identifiers(
    system_name: &str,
    controller: &str,
    hardware_type: Option<&str>,
    hardware_id: Option<&str>,
) -> BusIdentifiers {
    let sanitized_system = sanitize_identifier(system_name);
    let sanitized_controller = sanitize_identifier(controller);
    let sanitized_hw_type = sanitize_optional(hardware_type, "unknown");
    let sanitized_hw_id = sanitize_optional(hardware_id, "unknown");

    let redis_hash = match (hardware_type, hardware_id) {
        (Some(ht), Some(id)) if !ht.is_empty() && !id.is_empty() => {
            format!("{}_{}", sanitize_identifier(ht), sanitize_identifier(id))
        }
        (Some(ht), _) if !ht.is_empty() => sanitize_identifier(ht),
        (_, Some(id)) if !id.is_empty() => format!("canbus_{}", sanitize_identifier(id)),
        _ => sanitized_controller,
    };

    let base_path = format!(
        "{}/{}/{}",
        sanitized_system, sanitized_hw_type, sanitized_hw_id
    );

    BusIdentifiers {
        redis_hash,
        control_topic: format!("{}/controls", &base_path),
        measurement_topic: format!("{}/measurements", base_path),
    }
}

pub fn sanitize_identifier(input: &str) -> String {
    input
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
}

fn sanitize_optional<'a>(value: Option<&'a str>, fallback: &'a str) -> Cow<'a, str> {
    let candidate = value.unwrap_or(fallback);
    if candidate.is_empty() {
        return Cow::Borrowed(fallback);
    }
    if candidate.chars().all(|c| c.is_ascii_alphanumeric()) {
        Cow::Owned(candidate.to_string())
    } else {
        Cow::Owned(sanitize_identifier(candidate))
    }
}
