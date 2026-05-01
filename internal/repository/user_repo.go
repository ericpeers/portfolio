package repository

import (
	"context"
	"errors"

	"github.com/epeers/portfolio/internal/models"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrUserNotFound = errors.New("user not found")
	ErrEmailTaken   = errors.New("email already registered")
)

type UserRepository struct {
	pool *pgxpool.Pool
}

func NewUserRepository(pool *pgxpool.Pool) *UserRepository {
	return &UserRepository{pool: pool}
}

// GetByEmail returns the UserDTO and the stored password hash for the given email.
func (r *UserRepository) GetByEmail(ctx context.Context, email string) (*models.UserDTO, string, error) {
	var u models.UserDTO
	var hash string
	err := r.pool.QueryRow(ctx, `
		SELECT id, name, email, COALESCE(passwd, ''), role, organization_id, is_approved
		FROM dim_user
		WHERE email = $1
	`, email).Scan(&u.ID, &u.Name, &u.Email, &hash, &u.Role, &u.OrganizationID, &u.IsApproved)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, "", ErrUserNotFound
		}
		return nil, "", err
	}
	return &u, hash, nil
}

// GetByID returns the UserDTO for the given user ID (no password hash).
func (r *UserRepository) GetByID(ctx context.Context, id int64) (*models.UserDTO, error) {
	var u models.UserDTO
	err := r.pool.QueryRow(ctx, `
		SELECT id, name, email, role, organization_id, is_approved
		FROM dim_user
		WHERE id = $1
	`, id).Scan(&u.ID, &u.Name, &u.Email, &u.Role, &u.OrganizationID, &u.IsApproved)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrUserNotFound
		}
		return nil, err
	}
	return &u, nil
}

// Create inserts a new user with is_approved=FALSE, role='USER'. Returns the new user's ID.
func (r *UserRepository) Create(ctx context.Context, name, email, passwordHash string) (int64, error) {
	var id int64
	err := r.pool.QueryRow(ctx, `
		INSERT INTO dim_user (name, email, passwd, join_date, is_approved, role, updated_at)
		VALUES ($1, $2, $3, NOW(), FALSE, 'USER', NOW())
		RETURNING id
	`, name, email, passwordHash).Scan(&id)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return 0, ErrEmailTaken
		}
		return 0, err
	}
	return id, nil
}

// ListPending returns all users where is_approved = FALSE, ordered by id.
func (r *UserRepository) ListPending(ctx context.Context) ([]models.PendingUser, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, name, email, TO_CHAR(join_date, 'YYYY-MM-DD')
		FROM dim_user
		WHERE is_approved = FALSE
		ORDER BY id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var users []models.PendingUser
	for rows.Next() {
		var u models.PendingUser
		if err := rows.Scan(&u.ID, &u.Name, &u.Email, &u.JoinDate); err != nil {
			return nil, err
		}
		users = append(users, u)
	}
	return users, rows.Err()
}

// Approve flips is_approved = TRUE for the given user ID.
func (r *UserRepository) Approve(ctx context.Context, id int64) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE dim_user SET is_approved = TRUE, updated_at = NOW() WHERE id = $1
	`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrUserNotFound
	}
	return nil
}
