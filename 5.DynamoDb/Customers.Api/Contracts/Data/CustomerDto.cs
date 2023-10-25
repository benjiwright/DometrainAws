﻿using System.Text.Json.Serialization;

namespace Customers.Api.Contracts.Data;

public class CustomerDto
{
    [JsonPropertyName("pk")]
    public string Pk => Id.ToString();

    [JsonPropertyName("sk")]
    public string Sk => Id.ToString();

    public Guid Id { get; init; } = default!;

    public string GitHubUsername { get; init; } = default!;

    public string FullName { get; init; } = default!;

    public string Email { get; init; } = default!;

    public DateTime DateOfBirth { get; init; }

    public DateTime LastUpdated { get; set; }
}
