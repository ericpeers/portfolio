package tests

import (
	"strings"
	"testing"

	"github.com/epeers/portfolio/internal/handlers"
)

func TestParseMembershipCSV_HappyPath(t *testing.T) {
	csv := "ticker,percentage_or_shares\nAAPL,60\nMSFT,40\n"
	memberships, err := handlers.ParseMembershipCSV(strings.NewReader(csv))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(memberships) != 2 {
		t.Fatalf("expected 2 memberships, got %d", len(memberships))
	}
	if memberships[0].Ticker != "AAPL" || memberships[0].PercentageOrShares != 60 {
		t.Errorf("unexpected first membership: %+v", memberships[0])
	}
	if memberships[1].Ticker != "MSFT" || memberships[1].PercentageOrShares != 40 {
		t.Errorf("unexpected second membership: %+v", memberships[1])
	}
}

func TestParseMembershipCSV_MissingColumn(t *testing.T) {
	csv := "ticker,something_else\nAAPL,60\n"
	_, err := handlers.ParseMembershipCSV(strings.NewReader(csv))
	if err == nil {
		t.Fatal("expected error for missing column")
	}
	if !strings.Contains(err.Error(), "percentage_or_shares") {
		t.Errorf("expected error to mention missing column, got: %s", err.Error())
	}
}

func TestParseMembershipCSV_EmptyTicker(t *testing.T) {
	csv := "ticker,percentage_or_shares\n,60\n"
	_, err := handlers.ParseMembershipCSV(strings.NewReader(csv))
	if err == nil {
		t.Fatal("expected error for empty ticker")
	}
	if !strings.Contains(err.Error(), "row 2") {
		t.Errorf("expected error to mention row number, got: %s", err.Error())
	}
}

func TestParseMembershipCSV_InvalidFloat(t *testing.T) {
	csv := "ticker,percentage_or_shares\nAAPL,abc\n"
	_, err := handlers.ParseMembershipCSV(strings.NewReader(csv))
	if err == nil {
		t.Fatal("expected error for invalid float")
	}
	if !strings.Contains(err.Error(), "row 2") {
		t.Errorf("expected error to mention row number, got: %s", err.Error())
	}
}

func TestParseMembershipCSV_HeaderOnly(t *testing.T) {
	csv := "ticker,percentage_or_shares\n"
	memberships, err := handlers.ParseMembershipCSV(strings.NewReader(csv))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(memberships) != 0 {
		t.Errorf("expected 0 memberships, got %d", len(memberships))
	}
}

func TestParseMembershipCSV_CaseInsensitiveHeaders(t *testing.T) {
	csv := "Ticker,PERCENTAGE_OR_SHARES\nAAPL,60\n"
	memberships, err := handlers.ParseMembershipCSV(strings.NewReader(csv))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(memberships) != 1 {
		t.Fatalf("expected 1 membership, got %d", len(memberships))
	}
	if memberships[0].Ticker != "AAPL" || memberships[0].PercentageOrShares != 60 {
		t.Errorf("unexpected membership: %+v", memberships[0])
	}
}

func TestParseMembershipCSV_WhitespaceInValues(t *testing.T) {
	csv := "ticker,percentage_or_shares\n AAPL , 60.5 \n"
	memberships, err := handlers.ParseMembershipCSV(strings.NewReader(csv))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(memberships) != 1 {
		t.Fatalf("expected 1 membership, got %d", len(memberships))
	}
	if memberships[0].Ticker != "AAPL" {
		t.Errorf("expected ticker 'AAPL', got %q", memberships[0].Ticker)
	}
	if memberships[0].PercentageOrShares != 60.5 {
		t.Errorf("expected percentage 60.5, got %f", memberships[0].PercentageOrShares)
	}
}
