package services

import (
	"context"
	"errors"
	"time"

	"github.com/epeers/portfolio/internal/models"
	"github.com/epeers/portfolio/internal/repository"
	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"
)

// bcryptCost is the work factor passed to bcrypt. 10 is the OWASP-recommended minimum;
// increase as server hardware allows to raise the brute-force cost.
const bcryptCost = 10

var (
	ErrInvalidCredentials = errors.New("invalid credentials")
	ErrNotApproved        = errors.New("account pending approval")
)

type AuthService struct {
	userRepo  *repository.UserRepository
	jwtSecret []byte
}

func NewAuthService(userRepo *repository.UserRepository, jwtSecret string) *AuthService {
	return &AuthService{userRepo: userRepo, jwtSecret: []byte(jwtSecret)}
}

// Register hashes the password and creates a new unapproved user.
// Returns ErrEmailTaken if the email is already registered.
func (s *AuthService) Register(ctx context.Context, req models.RegisterRequest) (*models.UserDTO, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcryptCost)
	if err != nil {
		return nil, err
	}

	id, err := s.userRepo.Create(ctx, req.Name, req.Email, string(hash))
	if err != nil {
		return nil, err // ErrEmailTaken propagates as-is
	}

	return &models.UserDTO{
		ID:         id,
		Name:       req.Name,
		Email:      req.Email,
		Role:       "USER",
		IsApproved: false,
	}, nil
}

// Login verifies credentials and returns a signed JWT on success.
// Returns ErrInvalidCredentials for unknown email or wrong password.
// Returns ErrNotApproved if the account has not been approved.
func (s *AuthService) Login(ctx context.Context, req models.LoginRequest) (*models.AuthResponse, error) {
	user, hash, err := s.userRepo.GetByEmail(ctx, req.Email)
	if err != nil {
		if errors.Is(err, repository.ErrUserNotFound) {
			return nil, ErrInvalidCredentials
		}
		return nil, err
	}

	if bcrypt.CompareHashAndPassword([]byte(hash), []byte(req.Password)) != nil {
		return nil, ErrInvalidCredentials
	}

	if !user.IsApproved {
		return nil, ErrNotApproved
	}

	token, err := s.issueToken(user)
	if err != nil {
		return nil, err
	}

	return &models.AuthResponse{Token: token, User: *user}, nil
}

// GetUserByID returns the UserDTO for the given user ID.
func (s *AuthService) GetUserByID(ctx context.Context, id int64) (*models.UserDTO, error) {
	return s.userRepo.GetByID(ctx, id)
}

// ListPending returns all unapproved users ordered by ID.
func (s *AuthService) ListPending(ctx context.Context) ([]models.PendingUser, error) {
	return s.userRepo.ListPending(ctx)
}

// Approve approves the user with the given ID.
func (s *AuthService) Approve(ctx context.Context, id int64) error {
	return s.userRepo.Approve(ctx, id)
}

func (s *AuthService) issueToken(user *models.UserDTO) (string, error) {
	claims := models.JWTClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(24 * time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
		UserID: user.ID,
		Role:   user.Role,
		OrgID:  user.OrganizationID,
	}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(s.jwtSecret)
}
